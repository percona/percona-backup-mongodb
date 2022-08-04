package cli

import (
	"context"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"gopkg.in/yaml.v2"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/log"
)

type restoreOpts struct {
	bcp      string
	pitr     string
	pitrBase string
	wait     bool
	rsMap    string
}

type restoreRet struct {
	Name     string `json:"name,omitempty"`
	Snapshot string `json:"snapshot,omitempty"`
	PITR     string `json:"point-in-time,omitempty"`
	done     bool
	physical bool
	err      string
}

func (r restoreRet) HasError() bool {
	return r.err != ""
}

func (r restoreRet) String() string {
	switch {
	case r.done:
		m := "\nRestore successfully finished!\n"
		if r.physical {
			m += "Restart the cluster and pbm-agents, and run `pbm config --force-resync`"
		}
		return m
	case r.err != "":
		return "\n Error: " + r.err
	case r.Snapshot != "":
		return fmt.Sprintf("Restore of the snapshot from '%s' has started", r.Snapshot)
	case r.PITR != "":
		return fmt.Sprintf("Restore to the point in time '%s' has started", r.PITR)

	default:
		return ""
	}
}

func runRestore(cn *pbm.PBM, o *restoreOpts, outf outFormat) (fmt.Stringer, error) {
	rsMap, err := parseRSNamesMapping(o.rsMap)
	if err != nil {
		return nil, errors.WithMessage(err, "cannot parse replset mapping")
	}

	if o.pitr != "" && o.bcp != "" {
		return nil, errors.New("either a backup name or point in time should be set, non both together!")
	}

	switch {
	case o.bcp != "":
		m, err := restore(cn, o.bcp, rsMap, outf)
		if err != nil {
			return nil, err
		}
		if !o.wait {
			return restoreRet{Name: m.Name, Snapshot: o.bcp}, nil
		}

		typ := " logical restore.\nWaiting to finish"
		if m.Type == pbm.PhysicalBackup {
			typ = " physical restore.\nWaiting to finish"
		}
		fmt.Printf("Started%s", typ)
		err = waitRestore(cn, m)
		if err == nil {
			return restoreRet{
				done:     true,
				physical: m.Type == pbm.PhysicalBackup,
			}, nil
		}

		if serr, ok := err.(errRestoreFailed); ok {
			return restoreRet{err: serr.Error()}, nil
		}
		return restoreRet{err: fmt.Sprintf("%s.\n Try to check logs on node %s", err.Error(), m.Leader)}, nil
	case o.pitr != "":
		m, err := pitrestore(cn, o.pitr, o.pitrBase, rsMap, outf)
		if err != nil {
			return nil, err
		}
		if !o.wait || m == nil {
			return restoreRet{PITR: o.pitr}, nil
		}
		fmt.Print("Started.\nWaiting to finish")
		err = waitRestore(cn, m)
		if err != nil {
			return restoreRet{err: err.Error()}, nil
		}
		return restoreRet{
			done: true,
			PITR: o.pitr,
		}, nil
	default:
		return nil, errors.New("undefined restore state")
	}
}

func waitRestore(cn *pbm.PBM, m *pbm.RestoreMeta) error {
	ep, _ := cn.GetEpoch()
	stg, err := cn.GetStorage(cn.Logger().NewEvent(string(pbm.CmdRestore), m.Backup, m.OPID, ep.TS()))
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()

	var rmeta *pbm.RestoreMeta

	getMeta := cn.GetRestoreMeta
	if m.Type == pbm.PhysicalBackup {
		getMeta = func(name string) (*pbm.RestoreMeta, error) {
			return pbm.GetPhysRestoreMeta(name, stg)
		}
	}

	for range tk.C {
		fmt.Print(".")
		rmeta, err = getMeta(m.Name)
		if errors.Is(err, pbm.ErrNotFound) {
			continue
		}
		if err != nil {
			return errors.Wrap(err, "get restore metadata")
		}

		if m.Type == pbm.LogicalBackup {
			clusterTime, err := cn.ClusterTime()
			if err != nil {
				return errors.Wrap(err, "read cluster time")
			}
			if rmeta.Hb.T+pbm.StaleFrameSec < clusterTime.T {
				return errors.Errorf("operation staled, last heartbeat: %v", rmeta.Hb.T)
			}
		}

		switch rmeta.Status {
		case pbm.StatusDone:
			return nil
		case pbm.StatusError:
			return errRestoreFailed{fmt.Sprintf("operation failed with: %s", rmeta.Error)}
		}
	}

	return nil
}

type errRestoreFailed struct {
	string
}

func (e errRestoreFailed) Error() string {
	return e.string
}

func restore(cn *pbm.PBM, bcpName string, rsMapping map[string]string, outf outFormat) (*pbm.RestoreMeta, error) {
	bcp, err := cn.GetBackupMeta(bcpName)
	if errors.Is(err, pbm.ErrNotFound) {
		return nil, errors.Errorf("backup '%s' not found", bcpName)
	}
	if err != nil {
		return nil, errors.Wrap(err, "get backup data")
	}
	if bcp.Status != pbm.StatusDone {
		return nil, errors.Errorf("backup '%s' didn't finish successfully", bcpName)
	}

	err = checkConcurrentOp(cn)
	if err != nil {
		return nil, err
	}

	name := time.Now().UTC().Format(time.RFC3339Nano)
	err = cn.SendCmd(pbm.Cmd{
		Cmd: pbm.CmdRestore,
		Restore: &pbm.RestoreCmd{
			Name:       name,
			BackupName: bcpName,
			RSMap:      rsMapping,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "send command")
	}

	if outf != outText {
		return &pbm.RestoreMeta{
			Name:   name,
			Backup: bcpName,
			Type:   bcp.Type,
		}, nil
	}

	fmt.Printf("Starting restore %s from '%s'", name, bcpName)

	var (
		fn     getRestoreMetaFn
		ctx    context.Context
		cancel context.CancelFunc
	)

	// physical restore may take more time to start
	const waitPhysRestoreStart = time.Second * 120
	if bcp.Type == pbm.PhysicalBackup {
		ep, _ := cn.GetEpoch()
		stg, err := cn.GetStorage(cn.Logger().NewEvent(string(pbm.CmdRestore), bcpName, "", ep.TS()))
		if err != nil {
			return nil, errors.Wrap(err, "get storage")
		}

		fn = func(name string) (*pbm.RestoreMeta, error) {
			return pbm.GetPhysRestoreMeta(name, stg)
		}
		ctx, cancel = context.WithTimeout(context.Background(), waitPhysRestoreStart)
	} else {
		fn = cn.GetRestoreMeta
		ctx, cancel = context.WithTimeout(context.Background(), pbm.WaitActionStart)
	}
	defer cancel()

	return waitForRestoreStatus(ctx, cn, name, fn)
}

func parseTS(t string) (ts primitive.Timestamp, err error) {
	if si := strings.SplitN(t, ",", 2); len(si) == 2 {
		tt, err := strconv.ParseInt(si[0], 10, 64)
		if err != nil {
			return ts, errors.Wrap(err, "parse clusterTime T")
		}
		ti, err := strconv.ParseInt(si[1], 10, 64)
		if err != nil {
			return ts, errors.Wrap(err, "parse clusterTime I")
		}

		return primitive.Timestamp{T: uint32(tt), I: uint32(ti)}, nil
	}

	tsto, err := parseDateT(t)
	if err != nil {
		return ts, errors.Wrap(err, "parse date")
	}

	return primitive.Timestamp{T: uint32(tsto.Unix()), I: 0}, nil
}

func pitrestore(cn *pbm.PBM, t, base string, rsMap map[string]string, outf outFormat) (rmeta *pbm.RestoreMeta, err error) {
	ts, err := parseTS(t)
	if err != nil {
		return nil, err
	}

	err = checkConcurrentOp(cn)
	if err != nil {
		return nil, err
	}

	name := time.Now().UTC().Format(time.RFC3339Nano)
	err = cn.SendCmd(pbm.Cmd{
		Cmd: pbm.CmdPITRestore,
		PITRestore: &pbm.PITRestoreCmd{
			Name:  name,
			TS:    int64(ts.T),
			I:     int64(ts.I),
			Bcp:   base,
			RSMap: rsMap,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "send command")
	}

	if outf != outText {
		return nil, nil
	}

	fmt.Printf("Starting restore to the point in time '%s'", t)

	ctx, cancel := context.WithTimeout(context.Background(), pbm.WaitActionStart)
	defer cancel()

	return waitForRestoreStatus(ctx, cn, name, cn.GetRestoreMeta)
}

type getRestoreMetaFn func(name string) (*pbm.RestoreMeta, error)

func waitForRestoreStatus(ctx context.Context, cn *pbm.PBM, name string, getfn getRestoreMetaFn) (*pbm.RestoreMeta, error) {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	var err error
	meta := new(pbm.RestoreMeta)
	for {
		select {
		case <-tk.C:
			fmt.Print(".")
			meta, err = getfn(name)
			if errors.Is(err, pbm.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, errors.Wrap(err, "get metadata")
			}
			if meta == nil {
				continue
			}
			switch meta.Status {
			case pbm.StatusRunning, pbm.StatusDumpDone, pbm.StatusDone:
				return meta, nil
			case pbm.StatusError:
				rs := ""
				for _, s := range meta.Replsets {
					rs += fmt.Sprintf("\n- Restore on replicaset \"%s\" in state: %v", s.Name, s.Status)
					if s.Error != "" {
						rs += ": " + s.Error
					}
				}
				return nil, errors.New(meta.Error + rs)
			}
		case <-ctx.Done():
			rs := ""
			if meta != nil {
				for _, s := range meta.Replsets {
					rs += fmt.Sprintf("- Restore on replicaset \"%s\" in state: %v\n", s.Name, s.Status)
					if s.Error != "" {
						rs += ": " + s.Error
					}
				}
			}
			if rs == "" {
				rs = "<no replset has started restore>\n"
			}

			return nil, errors.New("no confirmation that restore has successfully started. Replsets status:\n" + rs)
		}
	}
}

type descrRestoreOpts struct {
	restore string
	cfg     string
}

type describeRestoreResult struct {
	Name               string           `yaml:"name" json:"name"`
	Backup             string           `yaml:"backup" json:"backup"`
	Type               pbm.BackupType   `yaml:"type" json:"type"`
	Status             pbm.Status       `yaml:"status" json:"status"`
	Error              *string          `yaml:"error,omitempty" json:"error,omitempty"`
	Replsets           []RestoreReplset `yaml:"replsets" json:"replsets"`
	OPID               string           `yaml:"opid" json:"opid"`
	StartTS            int64            `yaml:"-" json:"start_ts"`
	StartTime          string           `yaml:"start" json:"-"`
	LastTransitionTS   int64            `yaml:"-" json:"last_transition_ts"`
	LastTransitionTime string           `yaml:"last_transition_time" json:"-"`
}

type RestoreReplset struct {
	Name               string        `yaml:"name" json:"name"`
	Status             pbm.Status    `yaml:"status" json:"status"`
	Error              *string       `yaml:"error,omitempty" json:"error,omitempty"`
	LastTransitionTS   int64         `yaml:"-" json:"last_transition_ts"`
	LastTransitionTime string        `yaml:"last_transition_time" json:"-"`
	Nodes              []RestoreNode `yaml:"nodes,omitempty" json:"nodes,omitempty"`
}

type RestoreNode struct {
	Name               string     `yaml:"name" json:"name"`
	Status             pbm.Status `yaml:"status" json:"status"`
	Error              *string    `yaml:"error,omitempty" json:"error,omitempty"`
	LastTransitionTS   int64      `yaml:"-" json:"last_transition_ts"`
	LastTransitionTime string     `yaml:"last_transition_time" json:"-"`
}

func (r describeRestoreResult) String() string {
	b, err := yaml.Marshal(r)
	if err != nil {
		return fmt.Sprintln("error:", err)
	}

	return string(b)
}

func describeRestore(cn *pbm.PBM, o descrRestoreOpts) (fmt.Stringer, error) {
	var res describeRestoreResult
	var meta *pbm.RestoreMeta
	if o.cfg != "" {
		buf, err := ioutil.ReadFile(o.cfg)
		if err != nil {
			return nil, errors.Wrap(err, "unable to read config file")
		}

		var cfg pbm.Config
		err = yaml.UnmarshalStrict(buf, &cfg)
		if err != nil {
			return nil, errors.Wrap(err, "unable to  unmarshal config file")
		}

		stg, err := pbm.Storage(cfg, log.New(nil, "cli", "").NewEvent("", "", "", primitive.Timestamp{}))
		if err != nil {
			return nil, errors.Wrap(err, "get storage")
		}

		meta, err = pbm.GetPhysRestoreMeta(o.restore, stg)
		if err != nil && meta == nil {
			return nil, errors.Wrap(err, "get restore meta")
		}
	} else {
		var err error
		meta, err = cn.GetRestoreMeta(o.restore)
		if err != nil {
			return nil, errors.Wrap(err, "get restore meta")
		}
	}

	if meta == nil {
		return nil, errors.New("undefined restore meta")
	}

	res.Name = meta.Name
	res.Backup = meta.Backup
	res.Type = meta.Type
	res.Status = meta.Status
	res.OPID = meta.OPID
	res.StartTS = meta.StartTS
	res.StartTime = time.Unix(res.StartTS, 0).Format(time.RFC3339)
	res.LastTransitionTS = meta.LastTransitionTS
	res.LastTransitionTime = time.Unix(res.LastTransitionTS, 0).Format(time.RFC3339)
	if meta.Status == pbm.StatusError {
		res.Error = &meta.Error
	}

	for _, rs := range meta.Replsets {
		mrs := RestoreReplset{
			Name:               rs.Name,
			Status:             rs.Status,
			LastTransitionTS:   rs.LastTransitionTS,
			LastTransitionTime: time.Unix(rs.LastTransitionTS, 0).Format(time.RFC3339),
		}
		if rs.Status == pbm.StatusError {
			mrs.Error = &rs.Error
		}
		for _, node := range rs.Nodes {
			mnode := RestoreNode{
				Name:               node.Name,
				Status:             node.Status,
				LastTransitionTS:   node.LastTransitionTS,
				LastTransitionTime: time.Unix(node.LastTransitionTS, 0).Format(time.RFC3339),
			}
			if node.Status == pbm.StatusError {
				mnode.Error = &node.Error
			}

			mrs.Nodes = append(mrs.Nodes, mnode)
		}
		res.Replsets = append(res.Replsets, mrs)
	}

	return res, nil
}
