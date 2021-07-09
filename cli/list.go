package cli

import (
	"fmt"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/pkg/errors"
)

type listOpts struct {
	restore bool
	full    bool
	size    int64
}

type restoreStatus struct {
	StartTS     int64           `json:"start"`
	Status      pbm.Status      `json:"status"`
	Type        restoreListType `json:"type"`
	Snapshot    string          `json:"snapshot,omitempty"`
	PointInTime int64           `json:"point-in-time,omitempty"`
	Name        string          `json:"name,omitempty"`
	Error       string          `json:"error,omitempty"`
}

type restoreListType string

const (
	restorePITR     restoreListType = "pitr"
	restoreSnapshot restoreListType = "snapshot"
)

type restoreListOut []restoreStatus

func (r restoreListOut) String() string {
	s := fmt.Sprintln("Restores history:")
	for _, v := range r {
		var rprint, name string

		if v.Type == restoreSnapshot {
			name = v.Snapshot
		} else {
			name = "PITR: " + time.Unix(v.PointInTime, 0).UTC().Format(time.RFC3339)
		}
		if v.Name != "" {
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

func restoreList(cn *pbm.PBM, size int64, full bool) (restoreListOut, error) {
	rlist, err := cn.RestoresList(size)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get restore list")
	}

	var rout restoreListOut
	for i := len(rlist) - 1; i >= 0; i-- {
		r := rlist[i]

		rs := restoreStatus{
			StartTS:     r.StartTS,
			Status:      r.Status,
			Type:        restoreSnapshot,
			Snapshot:    r.Backup,
			PointInTime: r.PITR,
			Name:        r.Name,
			Error:       r.Error,
		}

		if r.PITR != 0 {
			rs.Type = restorePITR
		}

		rout = append(rout, rs)
	}

	return rout, nil
}
