package cli

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/pbm"
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
	Status           pbm.Status      `json:"status"`
	Type             restoreListType `json:"type"`
	Snapshot         string          `json:"snapshot,omitempty"`
	StartPointInTime int64           `json:"start-point-in-time,omitempty"`
	PointInTime      int64           `json:"point-in-time,omitempty"`
	Name             string          `json:"name,omitempty"`
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

		if v.Type == restoreSnapshot {
			name = fmt.Sprintf("%s [backup: %s]", v.Name, v.Snapshot)
		} else if v.Type == restoreReplay {
			name = fmt.Sprintf("Oplog Replay: %v - %v",
				time.Unix(v.StartPointInTime, 0).UTC().Format(time.RFC3339),
				time.Unix(v.PointInTime, 0).UTC().Format(time.RFC3339))
		} else {
			name = "PITR: " + time.Unix(v.PointInTime, 0).UTC().Format(time.RFC3339)
		}

		switch v.Status {
		case pbm.StatusDone:
			rprint = name + "\tdone"
		case pbm.StatusError:
			rprint = fmt.Sprintf("%s\tFailed with \"%s\"", name, v.Error)
		default:
			rprint = fmt.Sprintf("%s\tIn progress [%s] (Launched at %s)", name, v.Status, time.Unix(v.StartTS, 0).Format(time.RFC3339))
		}
		s += fmt.Sprintln(" ", rprint)
	}
	return s
}

func (r restoreListOut) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.list)
}

func runList(cn *pbm.PBM, l *listOpts) (fmt.Stringer, error) {
	rsMap, err := parseRSNamesMapping(l.rsMap)
	if err != nil {
		return nil, errors.WithMessage(err, "cannot parse replset mapping")
	}

	if l.restore {
		return restoreList(cn, int64(l.size))
	}
	// show message and skip when resync is running
	lk, err := findLock(cn, cn.GetLocks)
	if err == nil && lk != nil && lk.Type == pbm.CmdResync {
		return outMsg{"Storage resync is running. Backups list will be available after sync finishes."}, nil
	}

	return backupList(cn, l.size, l.full, l.unbacked, rsMap)
}

func restoreList(cn *pbm.PBM, size int64) (*restoreListOut, error) {
	rlist, err := cn.RestoresList(size)
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
	for _, b := range bl.Snapshots {
		kind := string(b.Type)
		if len(b.Namespaces) != 0 {
			kind += ", selective"
		}

		s += fmt.Sprintf("  %s <%s> [restore_to_time: %s]\n", b.Name, kind, fmtTS(int64(b.RestoreTS)))
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

func backupList(cn *pbm.PBM, size int, full, unbacked bool, rsMap map[string]string) (list backupListOut, err error) {
	list.Snapshots, err = getSnapshotList(cn, size, rsMap)
	if err != nil {
		return list, errors.Wrap(err, "get snapshots")
	}
	list.PITR.Ranges, list.PITR.RsRanges, err = getPitrList(cn, size, full, unbacked, rsMap)
	if err != nil {
		return list, errors.Wrap(err, "get PITR ranges")
	}

	list.PITR.On, err = cn.IsPITR()
	if err != nil {
		return list, errors.Wrap(err, "check if PITR is on")
	}

	return list, nil
}

func getSnapshotList(cn *pbm.PBM, size int, rsMap map[string]string) (s []snapshotStat, err error) {
	bcps, err := cn.BackupsList(int64(size))
	if err != nil {
		return nil, errors.Wrap(err, "unable to get backups list")
	}

	shards, err := cn.ClusterMembers()
	if err != nil {
		return nil, errors.Wrap(err, "get cluster members")
	}

	inf, err := cn.GetNodeInfo()
	if err != nil {
		return nil, errors.Wrap(err, "define cluster state")
	}

	// pbm.PBM is always connected either to config server or to the sole (hence main) RS
	// which the `confsrv` param in `bcpMatchCluster` is all about
	bcpsMatchCluster(bcps, shards, inf.SetName, rsMap)

	for i := len(bcps) - 1; i >= 0; i-- {
		b := bcps[i]

		if b.Status != pbm.StatusDone {
			continue
		}

		s = append(s, snapshotStat{
			Name:       b.Name,
			Namespaces: b.Namespaces,
			Status:     b.Status,
			RestoreTS:  int64(b.LastWriteTS.T),
			PBMVersion: b.PBMVersion,
			Type:       b.Type,
		})
	}

	return s, nil
}

// getPitrList shows only chunks derived from `Done` and compatible version's backups
func getPitrList(cn *pbm.PBM, size int, full, unbacked bool, rsMap map[string]string) (ranges []pitrRange, rsRanges map[string][]pitrRange, err error) {
	inf, err := cn.GetNodeInfo()
	if err != nil {
		return nil, nil, errors.Wrap(err, "define cluster state")
	}

	shards, err := cn.ClusterMembers()
	if err != nil {
		return nil, nil, errors.Wrap(err, "get cluster members")
	}

	now, err := cn.ClusterTime()
	if err != nil {
		return nil, nil, errors.Wrap(err, "get cluster time")
	}

	mapRevRS := pbm.MakeReverseRSMapFunc(rsMap)
	rsRanges = make(map[string][]pitrRange)
	var rstlines [][]pbm.Timeline
	for _, s := range shards {
		tlns, err := cn.PITRGetValidTimelines(mapRevRS(s.RS), now, nil)
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

	for _, tl := range pbm.MergeTimelines(rstlines...) {
		lastWrite, err := getBaseSnapshotLastWrite(cn, sh, rsMap, tl)
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

func getBaseSnapshotLastWrite(cn *pbm.PBM, sh map[string]bool, rsMap map[string]string, tl pbm.Timeline) (*primitive.Timestamp, error) {
	bcp, err := cn.GetFirstBackup(&primitive.Timestamp{T: tl.Start, I: 0})
	if err != nil {
		if !errors.Is(err, pbm.ErrNotFound) {
			return nil, errors.Wrapf(err, "get backup for timeline: %s", tl)
		}

		return nil, nil
	}
	if bcp == nil {
		return nil, nil
	}

	bcpMatchCluster(bcp, sh, pbm.MakeRSMapFunc(rsMap), pbm.MakeReverseRSMapFunc(rsMap))

	if bcp.Status != pbm.StatusDone {
		return nil, nil
	}

	return &bcp.LastWriteTS, nil
}

func splitByBaseSnapshot(lastWrite *primitive.Timestamp, tl pbm.Timeline) []pitrRange {
	if lastWrite == nil || (lastWrite.T < tl.Start || lastWrite.T > tl.End) {
		return []pitrRange{{Range: tl, NoBaseSnapshot: true}}
	}

	ranges := make([]pitrRange, 0, 1)

	if lastWrite.T > tl.Start {
		ranges = append(ranges, pitrRange{
			Range: pbm.Timeline{
				Start: tl.Start,
				End:   lastWrite.T,
			},
			NoBaseSnapshot: true,
		})
	}

	if lastWrite.T < tl.End {
		ranges = append(ranges, pitrRange{
			Range: pbm.Timeline{
				Start: lastWrite.T + 1,
				End:   tl.End,
			},
			NoBaseSnapshot: false,
		})
	}

	return ranges
}
