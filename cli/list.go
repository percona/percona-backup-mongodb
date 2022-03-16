package cli

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/version"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type listOpts struct {
	restore     bool
	oplogReplay bool
	full        bool
	oplogOnly   bool
	size        int
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
	full bool
}

func (r restoreListOut) String() string {
	s := fmt.Sprintln("Restores history:")
	for _, v := range r.list {
		var rprint, name string

		if v.Type == restoreSnapshot {
			name = v.Snapshot
		} else if v.Type == restoreReplay {
			name = fmt.Sprintf("Oplog Replay: %v - %v",
				time.Unix(v.StartPointInTime, 0).UTC().Format(time.RFC3339),
				time.Unix(v.PointInTime, 0).UTC().Format(time.RFC3339))
		} else {
			name = "PITR: " + time.Unix(v.PointInTime, 0).UTC().Format(time.RFC3339)
		}
		if r.full {
			name += fmt.Sprintf(" [%s]", v.Name)
		}

		switch v.Status {
		case pbm.StatusDone:
			rprint = name
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
	if l.restore {
		return restoreList(cn, int64(l.size), l.full)
	}
	// show message ans skip when resync is running
	lk, err := findLock(cn, cn.GetLocks)
	if err == nil && lk != nil && lk.Type == pbm.CmdResync {
		return outMsg{"Storage resync is running. Backups list will be available after sync finishes."}, nil
	}

	return backupList(cn, l.size, l.full, l.oplogOnly)
}

func restoreList(cn *pbm.PBM, size int64, full bool) (*restoreListOut, error) {
	rlist, err := cn.RestoresList(size)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get restore list")
	}

	rout := &restoreListOut{full: full}
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
	for _, b := range bl.Snapshots {
		s += fmt.Sprintf("  %s <%s> [complete: %s]\n", b.Name, b.Type, fmtTS(int64(b.StateTS)))
	}
	if bl.PITR.On {
		s += fmt.Sprintln("\nPITR <on>:")
	} else {
		s += fmt.Sprintln("\nPITR <off>:")
	}

	for _, r := range bl.PITR.Ranges {
		s += fmt.Sprintf("  %s - %s\n", fmtTS(int64(r.Range.Start)), fmtTS(int64(r.Range.End)))
	}
	if bl.PITR.RsRanges != nil {
		s += "\n"
		for n, r := range bl.PITR.RsRanges {
			s += fmt.Sprintf("  %s: %s\n", n, r)
		}
	}

	return s
}

func backupList(cn *pbm.PBM, size int, full, oplogOnly bool) (list backupListOut, err error) {
	list.Snapshots, err = getSnapshotList(cn, size)
	if err != nil {
		return list, errors.Wrap(err, "get snapshots")
	}
	list.PITR.Ranges, list.PITR.RsRanges, err = getPitrList(cn, size, full, oplogOnly)
	if err != nil {
		return list, errors.Wrap(err, "get PITR ranges")
	}

	list.PITR.On, err = cn.IsPITR()
	if err != nil {
		return list, errors.Wrap(err, "check if PITR is on")
	}

	return list, nil
}

func getSnapshotList(cn *pbm.PBM, size int) (s []snapshotStat, err error) {
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
	bcpsMatchCluster(bcps, shards, inf.SetName)

	for i := len(bcps) - 1; i >= 0; i-- {
		b := bcps[i]

		if b.Status != pbm.StatusDone {
			continue
		}
		if !version.Compatible(version.DefaultInfo.Version, b.PBMVersion) {
			continue
		}

		s = append(s, snapshotStat{
			Name:       b.Name,
			Status:     b.Status,
			StateTS:    int64(b.LastWriteTS.T),
			PBMVersion: b.PBMVersion,
			Type:       b.Type,
		})
	}

	return s, nil
}

// getPitrList shows only chunks derived from `Done` and compatible version's backups
func getPitrList(cn *pbm.PBM, size int, full, oplogOnly bool) (ranges []pitrRange, rsRanges map[string][]pitrRange, err error) {
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

	rsRanges = make(map[string][]pitrRange)
	var rstlines [][]pbm.Timeline
	for _, s := range shards {
		tlns, err := cn.PITRGetValidTimelines(s.RS, now, nil)
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

	sh := make(map[string]struct{}, len(shards))
	for _, s := range shards {
		sh[s.RS] = struct{}{}
	}

	var buf []string
	for _, tl := range pbm.MergeTimelines(rstlines...) {
		if !oplogOnly {
			bcp, err := cn.GetLastBackup(&primitive.Timestamp{T: tl.End, I: 0})
			if errors.Is(err, pbm.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, nil, errors.Wrapf(err, "get backup for timeline: %s", tl)
			}
			buf = buf[:0]
			bcpMatchCluster(bcp, sh, inf.SetName, &buf)

			if bcp.Status != pbm.StatusDone || !version.Compatible(version.DefaultInfo.Version, bcp.PBMVersion) {
				continue
			}
		}

		tl.Start++
		ranges = append(ranges, pitrRange{Range: tl})
	}

	return ranges, rsRanges, nil
}
