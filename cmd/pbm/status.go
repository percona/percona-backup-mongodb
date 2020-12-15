package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	plog "github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
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

func status(cn *pbm.PBM, f outFormat) {
	var o []statusSect

	sections := []struct {
		name string
		f    func(cn *pbm.PBM) (fmt.Stringer, error)
	}{
		{"Cluster", clusterStatus},
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
				return nil, errors.Errorf("conflicting ops running: [%s/%s::%s-%s] [%s/%s::%s-%s]",
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

func clusterStatus(cn *pbm.PBM) (fmt.Stringer, error) {
	inf, err := cn.GetNodeInfo()
	if err != nil {
		return nil, errors.Wrap(err, "get cluster info")
	}

	type clstr struct {
		rs    string
		nodes []string
	}
	topology := []clstr{
		{
			rs:    inf.SetName,
			nodes: inf.Hosts,
		},
	}

	if inf.IsSharded() {
		shrd, err := cn.GetShards()
		if err != nil {
			return nil, errors.Wrap(err, "get cluster shards")
		}
		for _, s := range shrd {
			topology = append(topology, clstr{
				rs:    s.ID,
				nodes: strings.Split(strings.TrimLeft(s.Host, s.ID+"/"), ","),
			})
		}
	}

	err = cn.AgentStatusGC()
	if err != nil {
		return nil, errors.Wrap(err, "clean-up stale agent statuses")
	}

	var ret cluster
	for _, shrd := range topology {
		lrs := rs{Name: shrd.rs}
		for i, n := range shrd.nodes {
			lrs.Nodes = append(lrs.Nodes, node{Host: shrd.rs + "/" + n})

			nd := &lrs.Nodes[i]

			stat, err := cn.GetAgentStatus(shrd.rs, n)
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

type pitr struct {
	Status string `json:"status"`
	Err    string `json:"error,omitempty"`
}

func (p pitr) String() string {
	s := fmt.Sprintf("Status [%s]", p.Status)
	if p.Err != "" {
		s += fmt.Sprintf("\n! ERROR while running PITR backup: %s", p.Err)
	}
	return s
}

func getPitrStatus(cn *pbm.PBM) (fmt.Stringer, error) {
	var p pitr
	on, err := cn.IsPITR()
	if err != nil {
		return p, errors.Wrap(err, "unable check PITR status")
	}
	if !on {
		p.Status = "OFF"
		return p, nil
	}
	p.Status = "ON"
	epch, err := cn.GetEpoch()
	if err != nil {
		return p, errors.Wrap(err, "get current epoch")
	}

	l, err := cn.LogGet(
		&plog.LogRequest{
			LogKeys: plog.LogKeys{
				Severity: plog.Error,
				Event:    string(pbm.CmdPITR),
				Epoch:    epch.TS(),
			},
		},
		1)
	if err != nil {
		return p, errors.Wrap(err, "get log records")
	}
	if len(l) > 0 {
		p.Err = l[0].String()
	}

	return p, nil
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

	// recheacing here means no conflict operation, hence all locks are the same,
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
	Type     pbm.StorageType `json:"type"`
	Path     string          `json:"path"`
	Region   string          `json:"region,omitempty"`
	Snapshot []snapshotStat  `json:"snapshot"`
	PITR     []pitrRange     `json:"pitrChunks,omitempty"`
}

type snapshotStat struct {
	Name    string     `json:"name"`
	Size    int64      `json:"size"`
	Status  pbm.Status `json:"status"`
	Err     string     `json:"error,omitempty"`
	StateTS int64      `json:"completeTS"`
}

type pitrRange struct {
	Range struct {
		Start int64 `json:"start"`
		End   int64 `json:"end"`
	} `json:"range"`
	Size int64 `json:"size"`
}

func (s storageStat) String() string {
	typ := "S3"
	if s.Type == pbm.StorageFilesystem {
		typ = "FS"
	}
	ret := fmt.Sprintf("%s %s %s\n", typ, s.Region, s.Path)
	if len(s.Snapshot) == 0 {
		return ret + "  (none)"
	}

	ret += fmt.Sprintln("  Snapshots:")
	for _, sn := range s.Snapshot {
		ret += fmt.Sprintf("    %s %s", sn.Name, fmtSize(sn.Size))
		switch sn.Status {
		case pbm.StatusDone:
			ret += fmt.Sprintf(" [complete: %s]", fmtTS(sn.StateTS))
		case pbm.StatusCancelled:
			ret += fmt.Sprintf(" [cancelled: %s]", fmtTS(sn.StateTS))
		case pbm.StatusError:
			ret += fmt.Sprintf(" [error: %s / %s]", sn.Err, fmtTS(sn.StateTS))
		default:
			ret += fmt.Sprintf(" [running: %s / %s]", sn.Status, fmtTS(sn.StateTS))
		}
		ret += "\n"
	}

	if len(s.PITR) == 0 {
		return ret
	}

	ret += fmt.Sprintln("  PITR chunks:")

	for _, sn := range s.PITR {
		ret += fmt.Sprintf("    %s - %s %s\n", fmtTS(sn.Range.Start), fmtTS(sn.Range.End), fmtSize(sn.Size))
	}

	return ret
}

func getStorageStat(cn *pbm.PBM) (fmt.Stringer, error) {
	var s storageStat

	cfg, err := cn.GetConfig()
	if err != nil {
		return s, errors.Wrap(err, "get config")
	}

	s.Type = cfg.Storage.Type

	switch cfg.Storage.Type {
	case pbm.StorageS3:
		var url []string
		if cfg.Storage.S3.EndpointURL != "" {
			url = append(url, cfg.Storage.S3.EndpointURL)
		}
		url = append(url, cfg.Storage.S3.Bucket)
		if cfg.Storage.S3.Prefix != "" {
			url = append(url, cfg.Storage.S3.Prefix)
		}
		s.Path = strings.Join(url, "/")
		s.Region = cfg.Storage.S3.Region
	case pbm.StorageFilesystem:
		s.Path = cfg.Storage.Filesystem.Path
	}

	bcps, err := cn.BackupsList(0)
	if err != nil {
		return s, errors.Wrap(err, "get backups list")
	}

	stg, err := cn.GetStorage(nil)
	if err != nil {
		return s, errors.Wrap(err, "get storage")
	}

	for _, bcp := range bcps {
		snpsht := snapshotStat{
			Name:    bcp.Name,
			Status:  bcp.Status,
			StateTS: bcp.LastTransitionTS,
		}

		switch bcp.Status {
		case pbm.StatusDone:
			snpsht.StateTS = int64(bcp.LastWriteTS.T)
			sz, err := getSnapshotSize(bcp.Replsets, stg)
			if err != nil {
				log.Println("ERROR:", err)
				continue
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
	inf, err := cn.GetNodeInfo()
	if err != nil {
		return pr, errors.Wrap(err, "define cluster state")
	}

	shards := []pbm.Shard{{ID: inf.SetName}}
	if inf.IsSharded() {
		s, err := cn.GetShards()
		if err != nil {
			return pr, errors.Wrap(err, "get shards")
		}
		shards = append(shards, s...)
	}

	now := time.Now().Unix()
	var rstlines [][]pbm.Timeline
	for _, s := range shards {
		tlns, err := cn.PITRGetValidTimelines(s.ID, now, stg)
		if err != nil {
			log.Printf("ERROR: get PITR timelines for %s replset: %v", s.ID, err)
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
