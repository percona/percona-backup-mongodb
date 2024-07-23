package main

import (
	"context"
	"encoding/json"
	"fmt"
	stdlog "log"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/lock"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/prio"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
	"github.com/percona/percona-backup-mongodb/pbm/slicer"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
	"github.com/percona/percona-backup-mongodb/pbm/util"
	"github.com/percona/percona-backup-mongodb/pbm/version"
	"github.com/percona/percona-backup-mongodb/sdk"
)

type statusOptions struct {
	rsMap    string
	sections []string
}

type statusOut struct {
	data   []*statusSect
	pretty bool
}

func (o statusOut) String() string {
	s := ""
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
	f        func(ctx context.Context, conn connect.Client) (fmt.Stringer, error)
}

func (f statusSect) String() string {
	return fmt.Sprintf("%s\n%s\n", sprinth(f.longName), f.Obj)
}

func (o statusOut) set(ctx context.Context, conn connect.Client, sfilter map[string]bool) error {
	for _, se := range o.data {
		if sfilter != nil && !sfilter[se.Name] {
			se.Obj = nil
			continue
		}

		var err error
		se.Obj, err = se.f(ctx, conn)
		if err != nil {
			return errors.Wrapf(err, "get status of %s", se.Name)
		}
	}

	return nil
}

func status(
	ctx context.Context,
	conn connect.Client,
	pbm sdk.Client,
	curi string,
	opts statusOptions,
	pretty bool,
) (fmt.Stringer, error) {
	rsMap, err := parseRSNamesMapping(opts.rsMap)
	if err != nil {
		return nil, errors.Wrap(err, "cannot parse replset mapping")
	}

	storageStatFn := func(ctx context.Context, conn connect.Client) (fmt.Stringer, error) {
		return getStorageStat(ctx, conn, pbm, rsMap)
	}

	out := statusOut{
		data: []*statusSect{
			{
				"cluster", "Cluster", nil,
				func(ctx context.Context, conn connect.Client) (fmt.Stringer, error) {
					return clusterStatus(ctx, conn, curi)
				},
			},
			{"pitr", "PITR incremental backup", nil, getPitrStatus},
			{"running", "Currently running", nil, getCurrOps},
			{"backups", "Backups", nil, storageStatFn},
		},
		pretty: pretty,
	}

	var sfilter map[string]bool
	if opts.sections != nil && len(opts.sections) > 0 {
		sfilter = make(map[string]bool)
		for _, s := range opts.sections {
			sfilter[s] = true
		}
	}

	err = out.set(ctx, conn, sfilter)

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

type RSRole string

const (
	RolePrimary   RSRole = "P"
	RoleSecondary RSRole = "S"
	RoleArbiter   RSRole = "A"
	RoleHidden    RSRole = "H"
	RoleDelayed   RSRole = "D"
)

type node struct {
	Host     string   `json:"host"`
	Ver      string   `json:"agent"`
	Role     RSRole   `json:"role"`
	PrioPITR float64  `json:"prio_pitr"`
	PrioBcp  float64  `json:"prio_backup"`
	OK       bool     `json:"ok"`
	Errs     []string `json:"errors,omitempty"`
}

func (n node) String() string {
	if n.Role == RoleArbiter {
		return fmt.Sprintf("%s [!Arbiter]: arbiter node is not supported", n.Host)
	}

	role := n.Role
	if role != RolePrimary && role != RoleSecondary {
		role = RoleSecondary
	}

	s := fmt.Sprintf("%s [%s], Bkp Prio: [%.1f], PITR Prio: [%.1f]: pbm-agent [%s]", n.Host, role, n.PrioBcp, n.PrioPITR, n.Ver)
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

func (c cluster) String() string {
	s := ""
	for _, rs := range c {
		s += fmt.Sprintf("%s:\n", rs.Name)
		for _, n := range rs.Nodes {
			s += fmt.Sprintf("  - %s\n", n)
		}
	}
	return s
}

func clusterStatus(ctx context.Context, conn connect.Client, uri string) (fmt.Stringer, error) {
	clstr, err := topo.ClusterMembers(ctx, conn.MongoClient())
	if err != nil {
		return nil, errors.Wrap(err, "get cluster members")
	}

	clusterTime, err := topo.GetClusterTime(ctx, conn)
	if err != nil {
		return nil, errors.Wrap(err, "read cluster time")
	}

	cfg, err := config.GetConfig(ctx, conn)
	if err != nil {
		return nil, errors.Wrap(err, "fetch config")
	}

	eg, ctx := errgroup.WithContext(ctx)
	m := sync.Mutex{}

	var ret cluster
	for _, c := range clstr {
		c := c

		eg.Go(func() error {
			client, err := directConnect(ctx, uri, c.Host)
			if err != nil {
				return errors.Wrapf(err, "connect to `%s` [%s]", c.RS, c.Host)
			}

			rsConfig, err := topo.GetReplSetConfig(ctx, client)
			if err != nil {
				_ = client.Disconnect(ctx)
				return errors.Wrapf(err, "get replset status for `%s`", c.RS)
			}
			info, err := topo.GetNodeInfo(ctx, client)
			// don't need the connection anymore despite the result
			_ = client.Disconnect(ctx)
			if err != nil {
				return errors.Wrap(err, "get node info")
			}

			lrs := rs{Name: c.RS}
			for i, n := range rsConfig.Members {
				lrs.Nodes = append(lrs.Nodes, node{Host: c.RS + "/" + n.Host})

				nd := &lrs.Nodes[i]
				switch {
				case n.Host == info.Primary:
					nd.Role = RolePrimary
				case n.ArbiterOnly:
					nd.Role = RoleArbiter
				case n.SecondaryDelayOld != 0 || n.SecondaryDelaySecs != 0:
					nd.Role = RoleDelayed
				case n.Hidden:
					nd.Role = RoleHidden
				}

				stat, err := topo.GetAgentStatus(ctx, conn, c.RS, n.Host)
				if errors.Is(err, mongo.ErrNoDocuments) {
					nd.Ver = "NOT FOUND"
					continue
				} else if err != nil {
					nd.Errs = append(nd.Errs, fmt.Sprintf("ERROR: get agent status: %v", err))
					continue
				}
				if stat.Heartbeat.T+defs.StaleFrameSec < clusterTime.T {
					nd.Errs = append(nd.Errs, fmt.Sprintf("ERROR: lost agent, last heartbeat: %v", stat.Heartbeat.T))
					continue
				}
				nd.Ver = "v" + stat.AgentVer
				nd.OK, nd.Errs = stat.OK()
				nd.PrioBcp = prio.CalcPriorityForAgent(stat, cfg.Backup.Priority, nil)
				nd.PrioPITR = prio.CalcPriorityForAgent(stat, cfg.PITR.Priority, nil)
			}

			m.Lock()
			ret = append(ret, lrs)
			m.Unlock()
			return nil
		})
	}

	err = eg.Wait()
	return ret, err
}

func directConnect(ctx context.Context, uri, hosts string) (*mongo.Client, error) {
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

	// Preserving the `replicaSet` parameter will cause an error
	// while connecting to the ConfigServer (mismatched replicaset names)
	query := curi.Query()
	query.Del("replicaSet")
	curi.RawQuery = query.Encode()
	curi.Host = host

	conn, err := connect.MongoConnect(ctx, curi.String(), connect.AppName("pbm-status"))
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
	InConf       bool     `json:"conf"`
	Running      bool     `json:"run"`
	RunningNodes []string `json:"nodes"`
	Err          string   `json:"error,omitempty"`
}

func (p pitrStat) String() string {
	status := "OFF"
	if p.InConf || p.Running {
		status = "ON"
	}
	s := fmt.Sprintf("Status [%s]", status)
	runningNodes := ""
	for _, n := range p.RunningNodes {
		runningNodes += fmt.Sprintf("%s; ", n)
	}
	if len(runningNodes) != 0 {
		s += fmt.Sprintf("\nRunning members: %s", runningNodes)
	}
	if p.Err != "" {
		s += fmt.Sprintf("\n! ERROR while running PITR backup: %s", p.Err)
	}
	return s
}

func getPitrStatus(ctx context.Context, conn connect.Client) (fmt.Stringer, error) {
	var p pitrStat
	var err error
	p.InConf, _, err = config.IsPITREnabled(ctx, conn)
	if err != nil {
		return p, errors.Wrap(err, "unable check PITR config status")
	}

	p.Running, err = oplog.IsOplogSlicing(ctx, conn)
	if err != nil {
		return p, errors.Wrap(err, "unable check PITR running status")
	}

	if p.InConf && p.Running {
		p.RunningNodes, err = oplog.GetAgentsWithACK(ctx, conn)
		if err != nil && err != errors.ErrNotFound {
			return p, errors.Wrap(err, "unable to fetch PITR running nodes")
		}
	}

	p.Err, err = getPitrErr(ctx, conn)

	return p, errors.Wrap(err, "check for errors")
}

func getPitrErr(ctx context.Context, conn connect.Client) (string, error) {
	epch, err := config.GetEpoch(ctx, conn)
	if err != nil {
		return "", errors.Wrap(err, "get current epoch")
	}

	shards, err := topo.ClusterMembers(ctx, conn.MongoClient())
	if err != nil {
		stdlog.Fatalf("Error: get cluster members: %v", err)
	}

	var errs []string
LOOP:
	for _, s := range shards {
		l, err := log.LogGetExactSeverity(ctx,
			conn,
			&log.LogRequest{
				LogKeys: log.LogKeys{
					Severity: log.Error,
					Event:    string(ctrl.CmdPITR),
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
		nl, err := log.LogGetExactSeverity(ctx,
			conn,
			&log.LogRequest{
				LogKeys: log.LogKeys{
					Severity: log.Debug,
					Event:    string(ctrl.CmdPITR),
					Epoch:    epch.TS(),
					RS:       s.RS,
				},
			},
			0)
		if err != nil {
			return "", errors.Wrap(err, "get debug log records")
		}
		for _, r := range nl.Data {
			if r.Msg == slicer.LogStartMsg && r.ObjID.Timestamp().After(l.Data[0].ObjID.Timestamp()) {
				continue LOOP
			}
		}

		errs = append(errs, l.Data[0].StringNode())
	}

	return strings.Join(errs, "; "), nil
}

type currOp struct {
	Type    ctrl.Command `json:"type,omitempty"`
	Name    string       `json:"name,omitempty"`
	StartTS int64        `json:"startTS,omitempty"`
	Status  string       `json:"status,omitempty"`
	OPID    string       `json:"opID,omitempty"`
}

func (c currOp) String() string {
	if c.Type == ctrl.CmdUndefined {
		return "(none)"
	}

	switch c.Type {
	default:
		return fmt.Sprintf("%s [op id: %s]", c.Type, c.OPID)
	case ctrl.CmdBackup, ctrl.CmdRestore:
		return fmt.Sprintf("%s \"%s\", started at %s. Status: %s. [op id: %s]",
			c.Type, c.Name, time.Unix((c.StartTS), 0).UTC().Format("2006-01-02T15:04:05Z"),
			c.Status, c.OPID,
		)
	}
}

func getCurrOps(ctx context.Context, conn connect.Client) (fmt.Stringer, error) {
	var r currOp

	// check for ops
	lk, err := findLock(ctx, conn, lock.GetLocks)
	if err != nil {
		return r, errors.Wrap(err, "get ops")
	}

	if lk == nil {
		// check for delete ops
		lk, err = findLock(ctx, conn, lock.GetOpLocks)
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
	// hence any lock in `lk` contains info on the current op
	switch r.Type {
	case ctrl.CmdBackup:
		bcp, err := backup.GetBackupByOPID(ctx, conn, r.OPID)
		if err != nil {
			return r, errors.Wrap(err, "get backup info")
		}
		r.Name = bcp.Name
		r.StartTS = bcp.StartTS
		r.Status = string(bcp.Status)
		switch bcp.Status {
		case defs.StatusRunning:
			r.Status = "snapshot backup"
		case defs.StatusDumpDone:
			r.Status = "oplog backup"
		}
	case ctrl.CmdRestore:
		rst, err := restore.GetRestoreMetaByOPID(ctx, conn, r.OPID)
		if err != nil {
			return r, errors.Wrap(err, "get restore info")
		}
		r.Name = rst.Backup
		r.StartTS = rst.StartTS
		r.Status = string(rst.Status)
		switch rst.Status {
		case defs.StatusRunning:
			r.Status = "snapshot restore"
		case defs.StatusDumpDone:
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

	sort.Slice(s.Snapshot, func(i, j int) bool {
		a, b := s.Snapshot[i], s.Snapshot[j]
		return a.RestoreTS > b.RestoreTS
	})

	for i := range s.Snapshot {
		ss := &s.Snapshot[i]
		var status string
		switch ss.Status {
		case defs.StatusDone:
			status = fmt.Sprintf("[restore_to_time: %s]", fmtTS(ss.RestoreTS))
		case defs.StatusCancelled:
			status = fmt.Sprintf("[!canceled: %s]", fmtTS(ss.RestoreTS))
		case defs.StatusError:
			if errors.Is(ss.Err, errIncompatible) {
				status = fmt.Sprintf("[incompatible: %s] [%s]", ss.Err.Error(), fmtTS(ss.RestoreTS))
			} else {
				status = fmt.Sprintf("[ERROR: %s] [%s]", ss.Err.Error(), fmtTS(ss.RestoreTS))
			}
		default:
			status = fmt.Sprintf("[running: %s / %s]", ss.Status, fmtTS(ss.RestoreTS))
		}

		t := string(ss.Type)
		if util.IsSelective(ss.Namespaces) {
			t += ", selective"
		} else if ss.Type == defs.IncrementalBackup && ss.SrcBackup == "" {
			t += ", base"
		}
		if ss.StoreName != "" {
			t += ", *"
		}
		ret += fmt.Sprintf("    %s %s <%s> %s\n", ss.Name, fmtSize(ss.Size), t, status)
	}

	if len(s.PITR.Ranges) == 0 {
		return ret
	}

	ret += fmt.Sprintf("  PITR chunks [%s]:\n", fmtSize(s.PITR.Size))

	sort.Slice(s.PITR.Ranges, func(i, j int) bool {
		a, b := s.PITR.Ranges[i], s.PITR.Ranges[j]
		return a.Range.End > b.Range.End
	})

	for _, sn := range s.PITR.Ranges {
		var v string
		if sn.Err != nil && !errors.Is(sn.Err, errors.ErrNotFound) {
			v = fmt.Sprintf(" !!! %s", sn.Err.Error())
		}
		f := ""
		if sn.NoBaseSnapshot {
			f = " (no base snapshot)"
		}
		ret += fmt.Sprintf("    %s - %s%s%s\n", fmtTS(int64(sn.Range.Start)), fmtTS(int64(sn.Range.End)), f, v)
	}

	return ret
}

func getStorageStat(
	ctx context.Context,
	conn connect.Client,
	pbm sdk.Client,
	rsMap map[string]string,
) (fmt.Stringer, error) {
	var s storageStat

	cfg, err := config.GetConfig(ctx, conn)
	if err != nil {
		return s, errors.Wrap(err, "get config")
	}

	s.Type = cfg.Storage.Typ()

	if cfg.Storage.Type == storage.S3 {
		s.Region = cfg.Storage.S3.Region
	}
	s.Path = cfg.Storage.Path()

	bcps, err := pbm.GetAllBackups(ctx)
	if err != nil {
		return s, errors.Wrap(err, "get backups list")
	}

	inf, err := topo.GetNodeInfoExt(ctx, conn.MongoClient())
	if err != nil {
		return s, errors.Wrap(err, "define cluster state")
	}
	ver, err := version.GetMongoVersion(ctx, conn.MongoClient())
	if err != nil {
		return nil, errors.Wrap(err, "get mongo version")
	}
	fcv, err := version.GetFCV(ctx, conn.MongoClient())
	if err != nil {
		return nil, errors.Wrap(err, "get featureCompatibilityVersion")
	}

	shards, err := topo.ClusterMembers(ctx, conn.MongoClient())
	if err != nil {
		return s, errors.Wrap(err, "get cluster members")
	}

	// pbm.PBM is always connected either to config server or to the sole (hence main) RS
	// which the `confsrv` param in `bcpMatchCluster` is all about
	bcpsMatchCluster(bcps, ver.VersionString, fcv, shards, inf.SetName, rsMap)

	stg, err := util.GetStorage(ctx, conn,
		log.FromContext(ctx).
			NewEvent("", "", "", primitive.Timestamp{}))
	if err != nil {
		return s, errors.Wrap(err, "get storage")
	}

	now, err := topo.GetClusterTime(ctx, conn)
	if err != nil {
		return nil, errors.Wrap(err, "get cluster time")
	}

	for _, bcp := range bcps {
		snpsht := snapshotStat{
			Name:       bcp.Name,
			Namespaces: bcp.Namespaces,
			Status:     bcp.Status,
			RestoreTS:  bcp.LastTransitionTS,
			PBMVersion: bcp.PBMVersion,
			Type:       bcp.Type,
			SrcBackup:  bcp.SrcBackup,
			StoreName:  bcp.Store.Name,
		}
		if err := bcp.Error(); err != nil {
			snpsht.Err = err
			snpsht.ErrString = err.Error()
		}

		switch bcp.Status {
		case defs.StatusError:
			if !errors.Is(snpsht.Err, errIncompatible) {
				break
			}
			fallthrough
		case defs.StatusDone:
			snpsht.RestoreTS = int64(bcp.LastWriteTS.T)
		case defs.StatusCancelled:
			// leave as it is, not to rewrite status with the `stuck` error
		default:
			if bcp.Hb.T+defs.StaleFrameSec < now.T {
				errStr := fmt.Sprintf("Backup stuck at `%v` stage, last beat ts: %d", bcp.Status, bcp.Hb.T)
				snpsht.Err = errors.New(errStr)
				snpsht.ErrString = errStr
				snpsht.Status = defs.StatusError
			}
		}

		bcp := bcp
		snpsht.Size, err = getBackupSize(&bcp, stg)
		if err != nil {
			snpsht.Err = err
			snpsht.ErrString = err.Error()
			snpsht.Status = defs.StatusError
		}

		s.Snapshot = append(s.Snapshot, snpsht)
	}

	s.PITR, err = getPITRranges(ctx, conn, bcps, rsMap)
	if err != nil {
		return s, errors.Wrap(err, "get PITR chunks")
	}

	return s, nil
}

func getPITRranges(
	ctx context.Context,
	conn connect.Client,
	bcps []backup.BackupMeta,
	rsMap map[string]string,
) (*pitrRanges, error) {
	shards, err := topo.ClusterMembers(ctx, conn.MongoClient())
	if err != nil {
		return nil, errors.Wrap(err, "get cluster members")
	}

	now, err := topo.GetClusterTime(ctx, conn)
	if err != nil {
		return nil, errors.Wrap(err, "get cluster time")
	}

	mapRevRS := util.MakeReverseRSMapFunc(rsMap)
	var size int64
	var rstlines [][]oplog.Timeline
	for _, s := range shards {
		tlns, err := oplog.PITRGetValidTimelines(ctx, conn, mapRevRS(s.RS), now)
		if err != nil {
			return nil, errors.Wrapf(err, "get PITR timelines for %s replset: %s", s.RS, err)
		}
		if tlns == nil {
			continue
		}

		rstlines = append(rstlines, tlns)

		for _, t := range tlns {
			size += t.Size
		}
	}

	sort.Slice(bcps, func(i, j int) bool {
		return bcps[i].LastWriteTS.Compare(bcps[j].LastWriteTS) == -1
	})

	var pr []pitrRange
	for _, tl := range oplog.MergeTimelines(rstlines...) {
		var bcplastWrite primitive.Timestamp

		for i := range bcps {
			bcp := &bcps[i]
			if !isValidBaseSnapshot(bcp) {
				continue
			}

			if bcp.LastWriteTS.T < tl.Start || bcp.FirstWriteTS.T > tl.End {
				continue
			}

			bcplastWrite = bcp.LastWriteTS
			break
		}

		pr = append(pr, splitByBaseSnapshot(bcplastWrite, tl)...)
	}

	return &pitrRanges{Ranges: pr, Size: size}, nil
}

func isValidBaseSnapshot(bcp *backup.BackupMeta) bool {
	if bcp.Status != defs.StatusDone {
		return false
	}
	if bcp.Type == defs.ExternalBackup {
		return false
	}
	if util.IsSelective(bcp.Namespaces) {
		return false
	}

	err := bcp.Error()
	if err == nil {
		return true
	}

	switch {
	case errors.Is(err, missedReplsetsError{}), errors.Is(err, incompatibleFCVVersionError{}):
		return true
	case errors.Is(err, incompatibleMongodVersionError{}):
		if bcp.Type == defs.LogicalBackup {
			return true
		}
	}

	return false
}

func getBackupSize(bcp *backup.BackupMeta, stg storage.Storage) (int64, error) {
	if bcp.Size > 0 {
		return bcp.Size, nil
	}

	var s int64
	var err error
	switch bcp.Status {
	case defs.StatusDone, defs.StatusCancelled, defs.StatusError:
		s, err = getLegacySnapshotSize(bcp, stg)
		if errors.Is(err, errMissedFile) && bcp.Status != defs.StatusDone {
			// canceled/failed backup can be incomplete. ignore
			err = nil
		}
	}

	return s, err
}

func getLegacySnapshotSize(bcp *backup.BackupMeta, stg storage.Storage) (int64, error) {
	switch bcp.Type {
	case defs.LogicalBackup:
		return getLegacyLogicalSize(bcp, stg)
	case defs.PhysicalBackup, defs.IncrementalBackup:
		return getLegacyPhysSize(bcp.Replsets)
	case defs.ExternalBackup:
		return 0, nil
	default:
		return 0, errors.Errorf("unknown backup type %s", bcp.Type)
	}
}

func getLegacyPhysSize(rsets []backup.BackupReplset) (int64, error) {
	var s int64
	for _, rs := range rsets {
		for _, f := range rs.Files {
			s += f.StgSize
		}
	}

	return s, nil
}

var errMissedFile = errors.New("missed file")

func getLegacyLogicalSize(bcp *backup.BackupMeta, stg storage.Storage) (int64, error) {
	var s int64
	var err error
	for _, rs := range bcp.Replsets {
		ds, er := stg.FileStat(rs.DumpName)
		if er != nil {
			if bcp.Status == defs.StatusDone || !errors.Is(er, storage.ErrNotExist) {
				return s, errors.Wrapf(er, "get file %s", rs.DumpName)
			}

			err = errMissedFile
		}

		op, er := stg.FileStat(rs.OplogName)
		if er != nil {
			if bcp.Status == defs.StatusDone || !errors.Is(er, storage.ErrNotExist) {
				return s, errors.Wrapf(er, "get file %s", rs.OplogName)
			}

			err = errMissedFile
		}

		s += ds.Size + op.Size
	}

	return s, err
}
