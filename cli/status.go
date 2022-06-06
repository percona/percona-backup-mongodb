package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona/percona-backup-mongodb/pbm"
	plog "github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/pitr"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/version"
)

type statusOut struct {
	data   []*statusSect
	pretty bool
}

func (o statusOut) String() (s string) {
	for _, sc := range o.data {
		if sc.Obj != nil {
			s += sc.String() + "\n"
		}
	}

	return s
}

func (o statusOut) MarshalJSON() ([]byte, error) {
	s := make(map[string]fmt.Stringer)
	for _, sc := range o.data {
		if sc.Obj != nil {
			s[sc.Name] = sc.Obj
		}
	}

	if o.pretty {
		return json.MarshalIndent(s, "", "  ")
	}
	return json.Marshal(s)
}

type statusSect struct {
	Name     string
	longName string
	Obj      fmt.Stringer
	f        func(cn *pbm.PBM) (fmt.Stringer, error)
}

func (f statusSect) String() string {
	return fmt.Sprintf("%s\n%s\n", sprinth(f.longName), f.Obj)
}

func (s statusOut) set(cn *pbm.PBM, curi string, sfilter map[string]bool) (err error) {
	for _, se := range s.data {
		if sfilter != nil && !sfilter[se.Name] {
			se.Obj = nil
			continue
		}

		se.Obj, err = se.f(cn)
		if err != nil {
			return errors.Wrapf(err, "get status of %s", se.Name)
		}
	}

	return nil
}

func status(cn *pbm.PBM, curi string, showSection *[]string, rsMapRaw string, pretty bool) (fmt.Stringer, error) {
	rsMap, err := parseRSNamesMapping(rsMapRaw)
	if err != nil {
		return nil, errors.WithMessage(err, "cannot parse replset mapping")
	}

	storageStatFn := func(cn *pbm.PBM) (fmt.Stringer, error) {
		return getStorageStat(cn, rsMap)
	}

	out := statusOut{
		data: []*statusSect{
			{
				"cluster", "Cluster", nil,
				func(cn *pbm.PBM) (fmt.Stringer, error) {
					return clusterStatus(cn, curi)
				},
			},
			{"pitr", "PITR incremental backup", nil, getPitrStatus},
			{"running", "Currently running", nil, getCurrOps},
			{"backups", "Backups", nil, storageStatFn},
		},
		pretty: pretty,
	}

	var sfilter map[string]bool
	if showSection != nil && len(*showSection) > 0 {
		sfilter = make(map[string]bool)
		for _, s := range *showSection {
			sfilter[s] = true
		}
	}

	err = out.set(cn, curi, sfilter)

	return out, err
}

func fmtSize(size int64) string {
	const (
		_          = iota
		KB float64 = 1 << (10 * iota)
		MB
		GB
		TB
	)

	s := float64(size)

	switch {
	case s >= TB:
		return fmt.Sprintf("%.2fTB", s/TB)
	case s >= GB:
		return fmt.Sprintf("%.2fGB", s/GB)
	case s >= MB:
		return fmt.Sprintf("%.2fMB", s/MB)
	case s >= KB:
		return fmt.Sprintf("%.2fKB", s/KB)
	}
	return fmt.Sprintf("%.2fB", s)
}

func sprinth(s string) string {
	return fmt.Sprintf("%s:\n%s", s, strings.Repeat("=", len(s)+1))
}

type cluster []rs

type rs struct {
	Name  string `json:"rs"`
	Nodes []node `json:"nodes"`
}

type node struct {
	Host string   `json:"host"`
	Ver  string   `json:"agent"`
	OK   bool     `json:"ok"`
	Errs []string `json:"errors,omitempty"`
}

func (n node) String() (s string) {
	s += fmt.Sprintf("%s: pbm-agent %v", n.Host, n.Ver)
	if n.OK {
		s += " OK"
		return s
	}
	s += " FAILED status:"
	for _, e := range n.Errs {
		s += fmt.Sprintf("\n      > ERROR with %s", e)
	}

	return s
}

func (c cluster) String() (s string) {
	for _, rs := range c {
		s += fmt.Sprintf("%s:\n", rs.Name)
		for _, n := range rs.Nodes {
			s += fmt.Sprintf("  - %s\n", n)
		}
	}
	return s
}

func clusterStatus(cn *pbm.PBM, uri string) (fmt.Stringer, error) {
	clstr, err := cn.ClusterMembers()
	if err != nil {
		return nil, errors.Wrap(err, "get cluster members")
	}

	clusterTime, err := cn.ClusterTime()
	if err != nil {
		return nil, errors.Wrap(err, "read cluster time")
	}

	var ret cluster

	for _, c := range clstr {
		rconn, err := connect(cn.Context(), uri, c.Host)
		if err != nil {
			return nil, errors.Wrapf(err, "connect to `%s` [%s]", c.RS, c.Host)
		}

		sstat, sterr := pbm.GetReplsetStatus(cn.Context(), rconn)

		// don't need the connection anymore despite the result
		rconn.Disconnect(cn.Context())

		if sterr != nil {
			return nil, errors.Wrapf(err, "get replset status for `%s`", c.RS)
		}
		lrs := rs{Name: c.RS}
		for i, n := range sstat.Members {
			lrs.Nodes = append(lrs.Nodes, node{Host: c.RS + "/" + n.Name})

			nd := &lrs.Nodes[i]

			stat, err := cn.GetAgentStatus(c.RS, n.Name)
			if errors.Is(err, mongo.ErrNoDocuments) {
				nd.Ver = "NOT FOUND"
				continue
			} else if err != nil {
				nd.Errs = append(nd.Errs, fmt.Sprintf("ERROR: get agent status: %v", err))
				continue
			}
			if stat.Heartbeat.T+pbm.StaleFrameSec < clusterTime.T {
				nd.Errs = append(nd.Errs, fmt.Sprintf("ERROR: lost agent, last heartbeat: %v", stat.Heartbeat.T))
				continue
			}
			nd.Ver = "v" + stat.Ver
			nd.OK, nd.Errs = stat.OK()
		}
		ret = append(ret, lrs)
	}

	return ret, nil
}

func connect(ctx context.Context, uri, hosts string) (*mongo.Client, error) {
	var host string
	chost := strings.Split(hosts, "/")
	if len(chost) > 1 {
		host = chost[1]
	} else {
		host = chost[0]
	}

	curi, err := url.Parse("mongodb://" + strings.Replace(uri, "mongodb://", "", 1))
	if err != nil {
		return nil, errors.Wrapf(err, "parse mongo-uri '%s'", uri)
	}

	// Preserving the `replicaSet` parameter will cause an error while connecting to the ConfigServer (mismatched replicaset names)
	query := curi.Query()
	query.Del("replicaSet")
	curi.RawQuery = query.Encode()
	curi.Host = host

	conn, err := mongo.NewClient(options.Client().ApplyURI(curi.String()).SetAppName("pbm-status"))
	if err != nil {
		return nil, errors.Wrap(err, "create mongo client")
	}
	err = conn.Connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "connect")
	}

	err = conn.Ping(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "ping")
	}

	return conn, nil
}

type pitrStat struct {
	InConf  bool   `json:"conf"`
	Running bool   `json:"run"`
	Err     string `json:"error,omitempty"`
}

func (p pitrStat) String() string {
	status := "OFF"
	if p.InConf || p.Running {
		status = "ON"
	}
	s := fmt.Sprintf("Status [%s]", status)
	if p.Err != "" {
		s += fmt.Sprintf("\n! ERROR while running PITR backup: %s", p.Err)
	}
	return s
}

func getPitrStatus(cn *pbm.PBM) (fmt.Stringer, error) {
	var p pitrStat
	var err error
	p.InConf, err = cn.IsPITR()
	if err != nil {
		return p, errors.Wrap(err, "unable check PITR config status")
	}

	p.Running, err = cn.PITRrun()
	if err != nil {
		return p, errors.Wrap(err, "unable check PITR running status")
	}

	p.Err, err = getPitrErr(cn)

	return p, errors.Wrap(err, "check for errors")
}

func getPitrErr(cn *pbm.PBM) (string, error) {
	epch, err := cn.GetEpoch()
	if err != nil {
		return "", errors.Wrap(err, "get current epoch")
	}

	shards, err := cn.ClusterMembers()
	if err != nil {
		log.Fatalf("Error: get cluster members: %v", err)
	}

	var errs []string
LOOP:
	for _, s := range shards {
		l, err := cn.LogGetExactSeverity(
			&plog.LogRequest{
				LogKeys: plog.LogKeys{
					Severity: plog.Error,
					Event:    string(pbm.CmdPITR),
					Epoch:    epch.TS(),
					RS:       s.RS,
				},
			},
			1)
		if err != nil {
			return "", errors.Wrap(err, "get log records")
		}

		if len(l.Data) == 0 {
			continue
		}

		// check if some node in the RS had successfully restarted slicing
		nl, err := cn.LogGetExactSeverity(
			&plog.LogRequest{
				LogKeys: plog.LogKeys{
					Severity: plog.Debug,
					Event:    string(pbm.CmdPITR),
					Epoch:    epch.TS(),
					RS:       s.RS,
				},
			},
			0)
		if err != nil {
			return "", errors.Wrap(err, "get debug log records")
		}
		for _, r := range nl.Data {
			if r.Msg == pitr.LogStartMsg && r.ObjID.Timestamp().After(l.Data[0].ObjID.Timestamp()) {
				continue LOOP
			}
		}

		errs = append(errs, l.Data[0].StringNode())
	}

	return strings.Join(errs, "; "), nil
}

type currOp struct {
	Type    pbm.Command `json:"type,omitempty"`
	Name    string      `json:"name,omitempty"`
	StartTS int64       `json:"startTS,omitempty"`
	Status  string      `json:"status,omitempty"`
	OPID    string      `json:"opID,omitempty"`
}

func (c currOp) String() string {
	if c.Type == pbm.CmdUndefined {
		return "(none)"
	}

	switch c.Type {
	default:
		return fmt.Sprintf("%s [op id: %s]", c.Type, c.OPID)
	case pbm.CmdBackup, pbm.CmdRestore, pbm.CmdPITRestore:
		return fmt.Sprintf("%s \"%s\", started at %s. Status: %s. [op id: %s]",
			c.Type, c.Name, time.Unix((c.StartTS), 0).UTC().Format("2006-01-02T15:04:05Z"),
			c.Status, c.OPID,
		)
	}
}

func getCurrOps(cn *pbm.PBM) (fmt.Stringer, error) {
	var r currOp

	// check for ops
	lk, err := findLock(cn, cn.GetLocks)
	if err != nil {
		return r, errors.Wrap(err, "get ops")
	}

	if lk == nil {
		// check for delete ops
		lk, err = findLock(cn, cn.GetOpLocks)
		if err != nil {
			return r, errors.Wrap(err, "get delete ops")
		}
	}

	if lk == nil {
		return r, nil
	}

	r = currOp{
		Type: lk.Type,
		OPID: lk.OPID,
	}

	// reaching here means no conflict operation, hence all locks are the same,
	// hence any lock in `lk` contais info on the current op
	switch r.Type {
	case pbm.CmdBackup:
		bcp, err := cn.GetBackupByOPID(r.OPID)
		if err != nil {
			return r, errors.Wrap(err, "get backup info")
		}
		r.Name = bcp.Name
		r.StartTS = bcp.StartTS
		r.Status = string(bcp.Status)
		switch bcp.Status {
		case pbm.StatusRunning:
			r.Status = "snapshot backup"
		case pbm.StatusDumpDone:
			r.Status = "oplog backup"
		}
	case pbm.CmdRestore, pbm.CmdPITRestore:
		rst, err := cn.GetRestoreMetaByOPID(r.OPID)
		if err != nil {
			return r, errors.Wrap(err, "get restore info")
		}
		r.Name = rst.Backup
		r.StartTS = rst.StartTS
		r.Status = string(rst.Status)
		switch rst.Status {
		case pbm.StatusRunning:
			r.Status = "snapshot restore"
		case pbm.StatusDumpDone:
			r.Status = "oplog restore"
		}
	}

	return r, nil
}

type storageStat struct {
	Type     string         `json:"type"`
	Path     string         `json:"path"`
	Region   string         `json:"region,omitempty"`
	Snapshot []snapshotStat `json:"snapshot"`
	PITR     *pitrRanges    `json:"pitrChunks,omitempty"`
}

type pitrRanges struct {
	Ranges []pitrRange `json:"pitrChunks,omitempty"`
	Size   int64       `json:"size"`
}

func (s storageStat) String() string {
	ret := fmt.Sprintf("%s %s %s\n", s.Type, s.Region, s.Path)
	if len(s.Snapshot) == 0 && len(s.PITR.Ranges) == 0 {
		return ret + "  (none)"
	}

	ret += fmt.Sprintln("  Snapshots:")
	for _, sn := range s.Snapshot {
		var status string
		switch sn.Status {
		case pbm.StatusDone:
			status = fmt.Sprintf("[complete: %s]", fmtTS(sn.StateTS))
		case pbm.StatusCancelled:
			status = fmt.Sprintf("[!cancelled: %s]", fmtTS(sn.StateTS))
		case pbm.StatusError:
			if errors.Is(sn.Err, errIncompatible) {
				status = fmt.Sprintf("[incompatible: %s] [%s]", sn.Err.Error(), fmtTS(sn.StateTS))
			} else {
				status = fmt.Sprintf("[ERROR: %s] [%s]", sn.Err.Error(), fmtTS(sn.StateTS))
			}
		default:
			status = fmt.Sprintf("[running: %s / %s]", sn.Status, fmtTS(sn.StateTS))
		}

		var v string
		if !version.Compatible(version.DefaultInfo.Version, sn.PBMVersion) {
			v = fmt.Sprintf(" !!! backup v%s is not compatible with PBM v%s", sn.PBMVersion, version.DefaultInfo.Version)
		}
		ret += fmt.Sprintf("    %s %s <%s> %s%s\n", sn.Name, fmtSize(sn.Size), sn.Type, status, v)
	}

	if len(s.PITR.Ranges) == 0 {
		return ret
	}

	ret += fmt.Sprintf("  PITR chunks [%s]:\n", fmtSize(s.PITR.Size))

	for _, sn := range s.PITR.Ranges {
		var v string
		if sn.Err != "" {
			v = fmt.Sprintf(" !!! %s", sn.Err)
		}
		f := ""
		if sn.NoBaseSnapshot {
			f = " (no base snapshot)"
		}
		ret += fmt.Sprintf("    %s - %s%s%s\n", fmtTS(int64(sn.Range.Start)), fmtTS(int64(sn.Range.End)), f, v)
	}

	return ret
}

func getStorageStat(cn *pbm.PBM, rsMap map[string]string) (fmt.Stringer, error) {
	var s storageStat

	cfg, err := cn.GetConfig()
	if err != nil {
		return s, errors.Wrap(err, "get config")
	}

	s.Type = cfg.Storage.Typ()

	if cfg.Storage.Type == pbm.StorageS3 {
		s.Region = cfg.Storage.S3.Region
	}
	s.Path = cfg.Storage.Path()

	bcps, err := cn.BackupsList(0)
	if err != nil {
		return s, errors.Wrap(err, "get backups list")
	}

	inf, err := cn.GetNodeInfo()
	if err != nil {
		return s, errors.Wrap(err, "define cluster state")
	}

	shards, err := cn.ClusterMembers()
	if err != nil {
		return s, errors.Wrap(err, "get cluster members")
	}

	// pbm.PBM is always connected either to config server or to the sole (hence main) RS
	// which the `confsrv` param in `bcpMatchCluster` is all about
	bcpsMatchCluster(bcps, shards, inf.SetName, rsMap)

	stg, err := cn.GetStorage(cn.Logger().NewEvent("", "", "", primitive.Timestamp{}))
	if err != nil {
		return s, errors.Wrap(err, "get storage")
	}

	now, err := cn.ClusterTime()
	if err != nil {
		return nil, errors.Wrap(err, "get cluster time")
	}

	for _, bcp := range bcps {
		snpsht := snapshotStat{
			Name:       bcp.Name,
			Status:     bcp.Status,
			StateTS:    bcp.LastTransitionTS,
			PBMVersion: bcp.PBMVersion,
			Type:       bcp.Type,
			Err:        bcp.Error,
		}

		switch bcp.Status {
		case pbm.StatusError:
			if !errors.Is(snpsht.Err, errIncompatible) {
				break
			}
			fallthrough
		case pbm.StatusDone:
			snpsht.StateTS = int64(bcp.LastWriteTS.T)
			var err error
			switch bcp.Type {
			case pbm.PhysicalBackup:
				snpsht.Size, err = getPhysSnapshotSize(&bcp, stg)
			default:
				snpsht.Size, err = getSnapshotSize(bcp.Replsets, stg)
			}
			if err != nil {
				snpsht.Err = err
				snpsht.Status = pbm.StatusError
			}
		case pbm.StatusCancelled:
			// leave as it is, not to rewrite status with the `stuck` error
		default:
			if bcp.Hb.T+pbm.StaleFrameSec < now.T {
				snpsht.Err = fmt.Errorf("Backup stuck at `%v` stage, last beat ts: %d", bcp.Status, bcp.Hb.T)
				snpsht.Status = pbm.StatusError
			}
		}
		s.Snapshot = append(s.Snapshot, snpsht)
	}

	s.PITR, err = getPITRranges(cn, stg, rsMap)
	if err != nil {
		return s, errors.Wrap(err, "get PITR chunks")
	}

	return s, nil
}

func getPITRranges(cn *pbm.PBM, stg storage.Storage, rsMap map[string]string) (*pitrRanges, error) {
	shards, err := cn.ClusterMembers()
	if err != nil {
		return nil, errors.Wrap(err, "get cluster members")
	}

	fl, err := stg.List(pbm.PITRfsPrefix, "")
	if err != nil {
		return nil, errors.Wrap(err, "get chunks list")
	}

	flist := make(map[string]int64)
	for _, f := range fl {
		flist[pbm.PITRfsPrefix+"/"+f.Name] = f.Size
	}

	now, err := cn.ClusterTime()
	if err != nil {
		return nil, errors.Wrap(err, "get cluster time")
	}

	mapRevRS := pbm.MakeReverseRSMapFunc(rsMap)
	var size int64
	var rstlines [][]pbm.Timeline
	for _, s := range shards {
		tlns, err := cn.PITRGetValidTimelines(mapRevRS(s.RS), now, flist)
		if err != nil {
			log.Printf("ERROR: get PITR timelines for %s replset: %v", s.RS, err)
			continue
		}
		if tlns == nil {
			continue
		}

		rstlines = append(rstlines, tlns)

		for _, t := range tlns {
			size += t.Size
		}
	}

	merged := pbm.MergeTimelines(rstlines...)

	var pr []pitrRange
	for i := len(merged) - 1; i >= 0; i-- {
		tl := merged[i]
		var rng pitrRange
		rng.Range.Start = tl.Start + 1
		rng.Range.End = tl.End

		bcp, err := cn.GetLastBackup(&primitive.Timestamp{T: tl.End, I: 0})
		if err != nil && errors.Is(err, pbm.ErrNotFound) {
			log.Printf("ERROR: get backup for timeline: %s", tl)
			rng.NoBaseSnapshot = true
		}
		if errors.Is(err, pbm.ErrNotFound) {
			rng.Err = "no backup found"
			rng.NoBaseSnapshot = true
		} else if !version.Compatible(version.DefaultInfo.Version, bcp.PBMVersion) {
			rng.Err = fmt.Sprintf("backup v%s is not compatible with PBM v%s", bcp.PBMVersion, version.DefaultInfo.Version)
			rng.NoBaseSnapshot = true
		}
		pr = append(pr, rng)
	}

	return &pitrRanges{Ranges: pr, Size: size}, nil
}

func getSnapshotSize(rsets []pbm.BackupReplset, stg storage.Storage) (s int64, err error) {
	for _, rs := range rsets {
		ds, err := stg.FileStat(rs.DumpName)
		if err != nil {
			return s, errors.Wrapf(err, "get file %s", rs.DumpName)
		}
		op, err := stg.FileStat(rs.OplogName)
		if err != nil {
			return s, errors.Wrapf(err, "get file %s", rs.OplogName)
		}

		s += ds.Size + op.Size
	}

	return s, nil
}

func getPhysSnapshotSize(bcp *pbm.BackupMeta, stg storage.Storage) (s int64, err error) {
	for _, rs := range bcp.Replsets {
		for _, f := range rs.Files {
			s += f.StgSize
		}
	}

	return s, nil
}
