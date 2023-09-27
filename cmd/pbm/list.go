package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/internal/config"
	"github.com/percona/percona-backup-mongodb/internal/context"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/lock"
	"github.com/percona/percona-backup-mongodb/internal/query"
	"github.com/percona/percona-backup-mongodb/internal/topo"
	"github.com/percona/percona-backup-mongodb/internal/util"
	"github.com/percona/percona-backup-mongodb/internal/version"
	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
)

type listOpts struct {
	restore  bool
	unbacked bool
	full     bool
	size     int
	rsMap    string
}

type restoreStatus struct {
	StartTS          int64           `json:"start"`
	Status           defs.Status     `json:"status"`
	Type             restoreListType `json:"type"`
	Snapshot         string          `json:"snapshot,omitempty"`
	StartPointInTime int64           `json:"start-point-in-time,omitempty"`
	PointInTime      int64           `json:"point-in-time,omitempty"`
	Name             string          `json:"name,omitempty"`
	Namespaces       []string        `json:"namespaces,omitempty"`
	Error            string          `json:"error,omitempty"`
}

type restoreListType string

const (
	restoreReplay   restoreListType = "replay"
	restorePITR     restoreListType = "pitr"
	restoreSnapshot restoreListType = "snapshot"
)

type restoreListOut struct {
	list []restoreStatus
}

func (r restoreListOut) String() string {
	s := fmt.Sprintln("Restores history:")
	for _, v := range r.list {
		var rprint, name string

		switch v.Type {
		case restoreSnapshot:
			t := string(v.Type)
			if util.IsSelective(v.Namespaces) {
				t += ", selective"
			}
			name = fmt.Sprintf("%s [backup: %s]", v.Name, t)
		case restoreReplay:
			name = fmt.Sprintf("Oplog Replay: %v - %v",
				time.Unix(v.StartPointInTime, 0).UTC().Format(time.RFC3339),
				time.Unix(v.PointInTime, 0).UTC().Format(time.RFC3339))
		default:
			n := time.Unix(v.PointInTime, 0).UTC().Format(time.RFC3339)
			if util.IsSelective(v.Namespaces) {
				n = ", selective"
			}
			name = fmt.Sprintf("PITR: %s [restore time: %s]", v.Name, n)
		}

		switch v.Status {
		case defs.StatusDone, defs.StatusPartlyDone:
			rprint = fmt.Sprintf("%s\t%s", name, v.Status)
		case defs.StatusError:
			rprint = fmt.Sprintf("%s\tFailed with \"%s\"", name, v.Error)
		default:
			rprint = fmt.Sprintf("%s\tIn progress [%s] (Launched at %s)",
				name, v.Status, time.Unix(v.StartTS, 0).Format(time.RFC3339))
		}
		s += fmt.Sprintln(" ", rprint)
	}
	return s
}

func (r restoreListOut) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.list)
}

func runList(ctx context.Context, cn *pbm.PBM, l *listOpts) (fmt.Stringer, error) {
	rsMap, err := parseRSNamesMapping(l.rsMap)
	if err != nil {
		return nil, errors.Wrap(err, "cannot parse replset mapping")
	}

	if l.restore {
		return restoreList(ctx, cn, int64(l.size))
	}
	// show message and skip when resync is running
	lk, err := findLock(ctx, cn, lock.GetLocks)
	if err == nil && lk != nil && lk.Type == defs.CmdResync {
		return outMsg{"Storage resync is running. Backups list will be available after sync finishes."}, nil
	}

	return backupList(ctx, cn, l.size, l.full, l.unbacked, rsMap)
}

func restoreList(ctx context.Context, cn *pbm.PBM, size int64) (*restoreListOut, error) {
	rlist, err := query.RestoresList(ctx, cn.Conn, size)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get restore list")
	}

	rout := &restoreListOut{}
	for i := len(rlist) - 1; i >= 0; i-- {
		r := rlist[i]

		rs := restoreStatus{
			StartTS:          r.StartTS,
			Status:           r.Status,
			Type:             restoreSnapshot,
			Snapshot:         r.Backup,
			StartPointInTime: r.StartPITR,
			PointInTime:      r.PITR,
			Name:             r.Name,
			Namespaces:       r.Namespaces,
			Error:            r.Error,
		}

		if r.PITR != 0 {
			if r.Backup == "" {
				rs.Type = restoreReplay
			} else {
				rs.Type = restorePITR
			}
		}

		rout.list = append(rout.list, rs)
	}

	return rout, nil
}

type backupListOut struct {
	Snapshots []snapshotStat `json:"snapshots"`
	PITR      struct {
		On       bool                   `json:"on"`
		Ranges   []pitrRange            `json:"ranges"`
		RsRanges map[string][]pitrRange `json:"rsRanges,omitempty"`
	} `json:"pitr"`
}

func (bl backupListOut) String() string {
	s := fmt.Sprintln("Backup snapshots:")

	sort.Slice(bl.Snapshots, func(i, j int) bool {
		return bl.Snapshots[i].RestoreTS < bl.Snapshots[j].RestoreTS
	})
	for i := range bl.Snapshots {
		b := &bl.Snapshots[i]
		t := string(b.Type)
		if util.IsSelective(b.Namespaces) {
			t += ", selective"
		} else if b.Type == defs.IncrementalBackup && b.SrcBackup == "" {
			t += ", base"
		}
		s += fmt.Sprintf("  %s <%s> [restore_to_time: %s]\n", b.Name, t, fmtTS(int64(b.RestoreTS)))
	}
	if bl.PITR.On {
		s += fmt.Sprintln("\nPITR <on>:")
	} else {
		s += fmt.Sprintln("\nPITR <off>:")
	}

	sort.Slice(bl.PITR.Ranges, func(i, j int) bool {
		return bl.PITR.Ranges[i].Range.End < bl.PITR.Ranges[j].Range.End
	})
	for _, r := range bl.PITR.Ranges {
		f := ""
		if r.NoBaseSnapshot {
			f = " (no base snapshot)"
		}
		s += fmt.Sprintf("  %s - %s%s\n", fmtTS(int64(r.Range.Start)), fmtTS(int64(r.Range.End)), f)
	}
	if bl.PITR.RsRanges != nil {
		s += "\n"
		for n, r := range bl.PITR.RsRanges {
			s += fmt.Sprintf("  %s: %s\n", n, r)
		}
	}

	return s
}

func backupList(
	ctx context.Context,
	cn *pbm.PBM,
	size int,
	full, unbacked bool,
	rsMap map[string]string,
) (backupListOut, error) {
	var list backupListOut
	var err error

	list.Snapshots, err = getSnapshotList(ctx, cn, size, rsMap)
	if err != nil {
		return list, errors.Wrap(err, "get snapshots")
	}
	list.PITR.Ranges, list.PITR.RsRanges, err = getPitrList(ctx, cn, size, full, unbacked, rsMap)
	if err != nil {
		return list, errors.Wrap(err, "get PITR ranges")
	}

	list.PITR.On, _, err = config.IsPITREnabled(ctx, cn.Conn)
	if err != nil {
		return list, errors.Wrap(err, "check if PITR is on")
	}

	return list, nil
}

func getSnapshotList(ctx context.Context, cn *pbm.PBM, size int, rsMap map[string]string) ([]snapshotStat, error) {
	bcps, err := query.BackupsList(ctx, cn.Conn, int64(size))
	if err != nil {
		return nil, errors.Wrap(err, "unable to get backups list")
	}

	shards, err := topo.ClusterMembers(ctx, cn.Conn.MongoClient())
	if err != nil {
		return nil, errors.Wrap(err, "get cluster members")
	}

	inf, err := topo.GetNodeInfoExt(ctx, cn.Conn.MongoClient())
	if err != nil {
		return nil, errors.Wrap(err, "define cluster state")
	}

	ver, err := version.GetMongoVersion(ctx, cn.Conn.MongoClient())
	if err != nil {
		return nil, errors.Wrap(err, "get mongo version")
	}
	fcv, err := version.GetFCV(ctx, cn.Conn.MongoClient())
	if err != nil {
		return nil, errors.Wrap(err, "get featureCompatibilityVersion")
	}

	// pbm.PBM is always connected either to config server or to the sole (hence main) RS
	// which the `confsrv` param in `bcpMatchCluster` is all about
	bcpsMatchCluster(bcps, ver.VersionString, fcv, shards, inf.SetName, rsMap)

	var s []snapshotStat
	for i := len(bcps) - 1; i >= 0; i-- {
		b := bcps[i]

		if b.Status != defs.StatusDone {
			continue
		}

		s = append(s, snapshotStat{
			Name:       b.Name,
			Namespaces: b.Namespaces,
			Status:     b.Status,
			RestoreTS:  int64(b.LastWriteTS.T),
			PBMVersion: b.PBMVersion,
			Type:       b.Type,
			SrcBackup:  b.SrcBackup,
		})
	}

	return s, nil
}

// getPitrList shows only chunks derived from `Done` and compatible version's backups
func getPitrList(
	ctx context.Context,
	cn *pbm.PBM,
	size int,
	full,
	unbacked bool,
	rsMap map[string]string,
) ([]pitrRange, map[string][]pitrRange, error) {
	inf, err := topo.GetNodeInfoExt(ctx, cn.Conn.MongoClient())
	if err != nil {
		return nil, nil, errors.Wrap(err, "define cluster state")
	}

	shards, err := topo.ClusterMembers(ctx, cn.Conn.MongoClient())
	if err != nil {
		return nil, nil, errors.Wrap(err, "get cluster members")
	}

	now, err := topo.GetClusterTime(ctx, cn.Conn)
	if err != nil {
		return nil, nil, errors.Wrap(err, "get cluster time")
	}

	mapRevRS := util.MakeReverseRSMapFunc(rsMap)
	rsRanges := make(map[string][]pitrRange)
	var rstlines [][]oplog.Timeline
	for _, s := range shards {
		tlns, err := oplog.PITRGetValidTimelines(ctx, cn.Conn, mapRevRS(s.RS), now)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "get PITR timelines for %s replset", s.RS)
		}

		if len(tlns) == 0 {
			continue
		}

		if size > 0 && size < len(tlns) {
			tlns = tlns[len(tlns)-size:]
		}

		if full {
			var rsrng []pitrRange
			for _, tln := range tlns {
				rsrng = append(rsrng, pitrRange{Range: tln})
			}
			rsRanges[s.RS] = rsrng
		}
		rstlines = append(rstlines, tlns)
	}

	sh := make(map[string]bool, len(shards))
	for _, s := range shards {
		sh[s.RS] = s.RS == inf.SetName
	}

	ranges := []pitrRange{}
	for _, tl := range oplog.MergeTimelines(rstlines...) {
		lastWrite, err := getBaseSnapshotLastWrite(ctx, cn, sh, rsMap, tl)
		if err != nil {
			return nil, nil, err
		}

		rs := splitByBaseSnapshot(lastWrite, tl)
		for i := range rs {
			if !unbacked && rs[i].NoBaseSnapshot {
				continue
			}

			ranges = append(ranges, rs[i])
		}
	}

	return ranges, rsRanges, nil
}

func getBaseSnapshotLastWrite(
	ctx context.Context,
	cn *pbm.PBM,
	sh map[string]bool,
	rsMap map[string]string,
	tl oplog.Timeline,
) (primitive.Timestamp, error) {
	bcp, err := query.GetFirstBackup(ctx, cn.Conn, &primitive.Timestamp{T: tl.Start, I: 0})
	if err != nil {
		if !errors.Is(err, errors.ErrNotFound) {
			return primitive.Timestamp{}, errors.Wrapf(err, "get backup for timeline: %s", tl)
		}

		return primitive.Timestamp{}, nil
	}
	if bcp == nil {
		return primitive.Timestamp{}, nil
	}

	ver, err := version.GetMongoVersion(ctx, cn.Conn.MongoClient())
	if err != nil {
		return primitive.Timestamp{}, errors.Wrap(err, "get mongo version")
	}
	fcv, err := version.GetFCV(ctx, cn.Conn.MongoClient())
	if err != nil {
		return primitive.Timestamp{}, errors.Wrap(err, "get featureCompatibilityVersion")
	}

	bcpMatchCluster(bcp, ver.VersionString, fcv, sh, util.MakeRSMapFunc(rsMap), util.MakeReverseRSMapFunc(rsMap))

	if bcp.Status != defs.StatusDone {
		return primitive.Timestamp{}, nil
	}

	return bcp.LastWriteTS, nil
}

func splitByBaseSnapshot(lastWrite primitive.Timestamp, tl oplog.Timeline) []pitrRange {
	if lastWrite.IsZero() || (lastWrite.T < tl.Start || lastWrite.T > tl.End) {
		return []pitrRange{{Range: tl, NoBaseSnapshot: true}}
	}

	ranges := make([]pitrRange, 0, 1)

	if lastWrite.T > tl.Start {
		ranges = append(ranges, pitrRange{
			Range: oplog.Timeline{
				Start: tl.Start,
				End:   lastWrite.T,
			},
			NoBaseSnapshot: true,
		})
	}

	if lastWrite.T < tl.End {
		ranges = append(ranges, pitrRange{
			Range: oplog.Timeline{
				Start: lastWrite.T + 1,
				End:   tl.End,
			},
			NoBaseSnapshot: false,
		})
	}

	return ranges
}
