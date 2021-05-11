package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
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

type statusSect struct {
	Name string
	Obj  fmt.Stringer
}

func (f statusSect) String() string {
	return fmt.Sprintf("%s\n%s\n", sprinth(f.Name), f.Obj)
}

func (f statusSect) MarshalJSON() ([]byte, error) {
	s := map[string]fmt.Stringer{f.Name: f.Obj}
	return json.Marshal(s)
}

type outFormat int

const (
	formatText outFormat = iota
	formatJSON
)

func status(cn *pbm.PBM, curi string, f outFormat) {
	var o []statusSect

	sections := []struct {
		name string
		f    func(cn *pbm.PBM) (fmt.Stringer, error)
	}{
		{"Cluster",
			func(cn *pbm.PBM) (fmt.Stringer, error) {
				return clusterStatus(cn, curi)
			},
		},
		{"PITR incremental backup", getPitrStatus},
		{"Currently running", getCurrOps},
		{"Backups", getStorageStat},
	}

	for _, s := range sections {
		obj, err := s.f(cn)
		if err != nil {
			log.Printf("ERROR: get status of %s: %v", s.name, err)
			continue
		}

		o = append(o, statusSect{s.name, obj})
	}

	switch f {
	case formatJSON:
		err := json.NewEncoder(os.Stdout).Encode(o)
		if err != nil {
			log.Println("ERROR: encode status:", err)
		}
	default:
		for _, s := range o {
			fmt.Printf("\n%s\n", strings.TrimSpace(s.String()))
		}
	}
}

func findLock(cn *pbm.PBM, fn func(*pbm.LockHeader) ([]pbm.LockData, error)) (*pbm.LockData, error) {
	locks, err := fn(&pbm.LockHeader{})
	if err != nil {
		return nil, errors.Wrap(err, "get locks")
	}

	ct, err := cn.ClusterTime()
	if err != nil {
		return nil, errors.Wrap(err, "get cluster time")
	}

	var lk *pbm.LockData
	for _, l := range locks {
		// We don't care about the PITR slicing here. It is a subject of other status sections
		if l.Type == pbm.CmdPITR || l.Heartbeat.T+pbm.StaleFrameSec < ct.T {
			continue
		}

		// Just check if all locks are for the same op
		//
		// It could happen that the healthy `lk` became stale by the time this check
		// or the op was finished and the new one was started. So the `l.Type != lk.Type`
		// would be true but for the legit reason (no error).
		// But chances for that are quite low and on the next run of `pbm status` everythin
		//  would be ok. So no reason to complicate code to avoid that.
		if lk != nil && l.OPID != lk.OPID {
			if err != nil {
				return nil, errors.Errorf("conflicting ops running: [%s/%s::%s-%s] [%s/%s::%s-%s]. This conflict may naturally resolve after 10 seconds",
					l.Replset, l.Node, l.Type, l.OPID,
					lk.Replset, lk.Node, lk.Type, lk.OPID,
				)
			}
		}

		lk = &l
	}

	return lk, nil
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

func fmtTS(ts int64) string {
	return time.Unix(ts, 0).UTC().Format("2006-01-02T15:04:05")
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
		s += fmt.Sprintf(" OK")
		return s
	}
	s += fmt.Sprintf(" FAILED status:")
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
	clstr, err := cn.ClusterMembers(nil)
	if err != nil {
		return nil, errors.Wrap(err, "get cluster members")
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

	shards, err := cn.ClusterMembers(nil)
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

		if len(l) == 0 {
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
		for _, r := range nl {
			if r.Msg == pitr.LogStartMsg && r.ObjID.Timestamp().After(l[0].ObjID.Timestamp()) {
				continue LOOP
			}
		}

		errs = append(errs, l[0].StringNode())
	}

	return strings.Join(errs, "; "), nil
}

type currOp struct {
	Type    pbm.Command `json:"type"`
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
	PITR     []pitrRange    `json:"pitrChunks,omitempty"`
}

type snapshotStat struct {
	Name       string     `json:"name"`
	Size       int64      `json:"size"`
	Status     pbm.Status `json:"status"`
	Err        string     `json:"error,omitempty"`
	StateTS    int64      `json:"completeTS"`
	PBMVersion string     `json:"pbmVersion"`
}

type pitrRange struct {
	Err   string `json:"error,omitempty"`
	Range struct {
		Start int64 `json:"start"`
		End   int64 `json:"end"`
	} `json:"range"`
	Size int64 `json:"size"`
}

func (s storageStat) String() string {
	ret := fmt.Sprintf("%s %s %s\n", s.Type, s.Region, s.Path)
	if len(s.Snapshot) == 0 {
		return ret + "  (none)"
	}

	ret += fmt.Sprintln("  Snapshots:")
	for _, sn := range s.Snapshot {
		var status string
		switch sn.Status {
		case pbm.StatusDone:
			status = fmt.Sprintf(" [complete: %s]", fmtTS(sn.StateTS))
		case pbm.StatusCancelled:
			status = fmt.Sprintf(" [!cancelled: %s]", fmtTS(sn.StateTS))
		case pbm.StatusError:
			status = fmt.Sprintf(" [ERROR: %s] [%s]", sn.Err, fmtTS(sn.StateTS))
		default:
			status = fmt.Sprintf(" [running: %s / %s]", sn.Status, fmtTS(sn.StateTS))
		}

		var v string
		if !version.Compatible(version.DefaultInfo.Version, sn.PBMVersion) {
			v = fmt.Sprintf(" !!! backup v%s is not compatible with PBM v%s", sn.PBMVersion, version.DefaultInfo.Version)
		}
		ret += fmt.Sprintf("    %s %s%s%s\n", sn.Name, fmtSize(sn.Size), status, v)
	}

	if len(s.PITR) == 0 {
		return ret
	}

	ret += fmt.Sprintln("  PITR chunks:")

	for _, sn := range s.PITR {
		var v string
		if sn.Err != "" {
			v = fmt.Sprintf(" !!! %s", sn.Err)
		}
		ret += fmt.Sprintf("    %s - %s %s%s\n", fmtTS(sn.Range.Start), fmtTS(sn.Range.End), fmtSize(sn.Size), v)
	}

	return ret
}

func getStorageStat(cn *pbm.PBM) (fmt.Stringer, error) {
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

	shards, err := cn.ClusterMembers(inf)
	if err != nil {
		return s, errors.Wrap(err, "get cluster members")
	}

	// pbm.PBM is always connected either to config server or to the sole (hence main) RS
	// which the `confsrv` param in `bcpMatchCluster` is all about
	bcpMatchCluster(bcps, shards, inf.SetName)

	stg, err := cn.GetStorage(cn.Logger().NewEvent("", "", "", primitive.Timestamp{}))
	if err != nil {
		return s, errors.Wrap(err, "get storage")
	}

	for _, bcp := range bcps {
		snpsht := snapshotStat{
			Name:       bcp.Name,
			Status:     bcp.Status,
			StateTS:    bcp.LastTransitionTS,
			PBMVersion: bcp.PBMVersion,
		}

		switch bcp.Status {
		case pbm.StatusDone:
			snpsht.StateTS = int64(bcp.LastWriteTS.T)
			sz, err := getSnapshotSize(bcp.Replsets, stg)
			if err != nil {
				snpsht.Err = err.Error()
				snpsht.Status = pbm.StatusError
			}
			snpsht.Size = sz
		case pbm.StatusError:
			snpsht.Err = bcp.Error
		}
		s.Snapshot = append(s.Snapshot, snpsht)
	}

	s.PITR, err = getPITRranges(cn, stg)
	if err != nil {
		return s, errors.Wrap(err, "get PITR chunks")
	}

	return s, nil
}

func getPITRranges(cn *pbm.PBM, stg storage.Storage) (pr []pitrRange, err error) {
	shards, err := cn.ClusterMembers(nil)
	if err != nil {
		return pr, errors.Wrap(err, "get cluster members")
	}

	fl, err := stg.List(pbm.PITRfsPrefix, "")
	if err != nil {
		return pr, errors.Wrap(err, "get chunks list")
	}

	flist := make(map[string]int64)
	for _, f := range fl {
		flist[pbm.PITRfsPrefix+"/"+f.Name] = f.Size
	}
	now := time.Now().Unix()
	var rstlines [][]pbm.Timeline
	for _, s := range shards {
		tlns, err := cn.PITRGetValidTimelines(s.RS, now, flist)
		if err != nil {
			log.Printf("ERROR: get PITR timelines for %s replset: %v", s.RS, err)
			continue
		}
		rstlines = append(rstlines, tlns)
	}

	merged := pbm.MergeTimelines(rstlines...)

	for i := len(merged) - 1; i >= 0; i-- {
		tl := merged[i]
		var rng pitrRange
		rng.Range.Start = int64(tl.Start)
		rng.Range.End = int64(tl.End)
		rng.Size = tl.Size

		bcp, err := cn.GetLastBackup(&primitive.Timestamp{T: tl.End, I: 0})
		if err != nil {
			log.Printf("ERROR: get backup for timeline: %s", tl)
			continue
		}
		if bcp == nil {
			rng.Err = "no backup found"
		} else if !version.Compatible(version.DefaultInfo.Version, bcp.PBMVersion) {
			rng.Err = fmt.Sprintf("backup v%s is not compatible with PBM v%s", bcp.PBMVersion, version.DefaultInfo.Version)
		}
		pr = append(pr, rng)
	}

	return pr, nil
}

func getSnapshotSize(rsets []pbm.BackupReplset, stg storage.Storage) (s int64, err error) {
	for _, rs := range rsets {
		ds, err := stg.FileStat(rs.DumpName)
		if err != nil {
			return s, errors.Wrapf(err, "get file %s", rs.DumpName)
		}

		s += ds.Size
	}

	return s, nil
}
