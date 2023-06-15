package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"gopkg.in/yaml.v2"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

type restoreOpts struct {
	bcp      string
	pitr     string
	pitrBase string
	wait     bool
	extern   bool
	ns       string
	rsMap    string
	conf     string
	ts       string
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
		if r.physical {
			return fmt.Sprintf(`
Restore of the snapshot from '%s' has started.
Check restore status with: pbm describe-restore %s -c </path/to/pbm.conf.yaml>
No other pbm command is available while the restore is running!
`,
				r.Snapshot, r.Name)
		}
		return fmt.Sprintf("Restore of the snapshot from '%s' has started", r.Snapshot)
	case r.PITR != "":
		return fmt.Sprintf("Restore to the point in time '%s' has started", r.PITR)

	default:
		return ""
	}
}

type externRestoreRet struct {
	Name     string `json:"name,omitempty"`
	Snapshot string `json:"snapshot,omitempty"`
}

func (r externRestoreRet) String() string {
	return fmt.Sprintf(`
	Ready to copy data to the nodes data directory.
	After the copy is done, run: pbm restore-finish %s -c </path/to/pbm.conf.yaml>
	Check restore status with: pbm describe-restore %s -c </path/to/pbm.conf.yaml>
	No other pbm command is available while the restore is running!
	`,
		r.Name, r.Name)
}

func runRestore(cn *pbm.PBM, o *restoreOpts, outf outFormat) (fmt.Stringer, error) {
	nss, err := parseCLINSOption(o.ns)
	if err != nil {
		return nil, errors.WithMessage(err, "parse --ns option")
	}

	rsMap, err := parseRSNamesMapping(o.rsMap)
	if err != nil {
		return nil, errors.WithMessage(err, "cannot parse replset mapping")
	}

	if o.pitr != "" && o.bcp != "" {
		return nil, errors.New("either a backup name or point in time should be set, non both together!")
	}

	clusterTime, err := cn.ClusterTime()
	if err != nil {
		return nil, errors.Wrap(err, "read cluster time")
	}
	tdiff := time.Now().Unix() - int64(clusterTime.T)

	switch {
	case o.bcp != "" || o.extern:
		m, err := restore(cn, o, nss, rsMap, outf)
		if err != nil {
			return nil, err
		}
		if o.extern && outf == outText {
			err = waitRestore(cn, m, pbm.StatusCopyReady, tdiff)
			if err != nil {
				return nil, errors.Wrap(err, "waiting for the `copyReady` status")
			}

			return externRestoreRet{Name: m.Name, Snapshot: o.bcp}, nil
		}
		if !o.wait {
			return restoreRet{
				Name:     m.Name,
				Snapshot: o.bcp,
				physical: m.Type == pbm.PhysicalBackup || m.Type == pbm.IncrementalBackup,
			}, nil
		}

		typ := " logical restore.\nWaiting to finish"
		if m.Type == pbm.PhysicalBackup {
			typ = " physical restore.\nWaiting to finish"
		}
		fmt.Printf("Started%s", typ)
		err = waitRestore(cn, m, pbm.StatusDone, tdiff)
		if err == nil {
			return restoreRet{
				done:     true,
				physical: m.Type == pbm.PhysicalBackup || m.Type == pbm.IncrementalBackup,
			}, nil
		}

		if serr, ok := err.(errRestoreFailed); ok {
			return restoreRet{err: serr.Error()}, nil
		}
		return restoreRet{err: fmt.Sprintf("%s.\n Try to check logs on node %s", err.Error(), m.Leader)}, nil
	case o.pitr != "":
		m, err := pitrestore(cn, o.pitr, o.pitrBase, nss, rsMap, outf)
		if err != nil {
			return nil, err
		}
		if !o.wait {
			return restoreRet{PITR: o.pitr, Name: m.Name}, nil
		}
		fmt.Print("Started.\nWaiting to finish")
		err = waitRestore(cn, m, pbm.StatusDone, tdiff)
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

// We rely on heartbeats in error detection in case of all nodes failed,
// comparing heartbeats with the current cluster time for logical restores.
// But for physical ones, the cluster by this time is down. So we compare with
// the wall time taking into account a time skew (wallTime - clusterTime) taken
// when the cluster time was still available.
func waitRestore(cn *pbm.PBM, m *pbm.RestoreMeta, status pbm.Status, tskew int64) error {
	ep, _ := cn.GetEpoch()
	l := cn.Logger().NewEvent(string(pbm.CmdRestore), m.Backup, m.OPID, ep.TS())
	stg, err := cn.GetStorage(l)
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()

	var rmeta *pbm.RestoreMeta

	getMeta := cn.GetRestoreMeta
	if m.Type == pbm.PhysicalBackup || m.Type == pbm.IncrementalBackup {
		getMeta = func(name string) (*pbm.RestoreMeta, error) {
			return pbm.GetPhysRestoreMeta(name, stg, l)
		}
	}

	var ctime uint32
	frameSec := pbm.StaleFrameSec
	if m.Type != pbm.LogicalBackup {
		frameSec = 60 * 3
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

		switch rmeta.Status {
		case status, pbm.StatusDone, pbm.StatusPartlyDone:
			return nil
		case pbm.StatusError:
			return errRestoreFailed{fmt.Sprintf("operation failed with: %s", rmeta.Error)}
		}

		if m.Type == pbm.LogicalBackup {
			clusterTime, err := cn.ClusterTime()
			if err != nil {
				return errors.Wrap(err, "read cluster time")
			}
			ctime = clusterTime.T
		} else {
			ctime = uint32(time.Now().Unix() + tskew)
		}

		if rmeta.Hb.T+frameSec < ctime {
			return errors.Errorf("operation staled, last heartbeat: %v", rmeta.Hb.T)
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

func checkBackup(cn *pbm.PBM, o *restoreOpts) (pbm.BackupType, error) {
	if o.extern && o.bcp == "" {
		return pbm.ExternalBackup, nil
	}

	bcp, err := cn.GetBackupMeta(o.bcp)
	if errors.Is(err, pbm.ErrNotFound) {
		return "", errors.Errorf("backup '%s' not found", o.bcp)
	}
	if err != nil {
		return "", errors.Wrap(err, "get backup data")
	}
	if len(nss) != 0 && bcp.Type != pbm.LogicalBackup {
		return nil, errors.New("--ns flag is only allowed for logical restore")
	}
	if bcp.Status != pbm.StatusDone {
		return "", errors.Errorf("backup '%s' didn't finish successfully", o.bcp)
	}

	return bcp.Type, nil
}

func restore(cn *pbm.PBM, o *restoreOpts, nss []string, rsMapping map[string]string, outf outFormat) (*pbm.RestoreMeta, error) {
	bcpType, err := checkBackup(cn, o)
	if err != nil {
		return nil, err
	}
	err = checkConcurrentOp(cn)
	if err != nil {
		return nil, err
	}

	restore := &pbm.RestoreCmd{
		Name:       time.Now().UTC().Format(time.RFC3339Nano),
		BackupName: o.bcp,
		Namespaces: nss,
		RSMap:      rsMapping,
		External:   o.extern,
	}
	if o.ts != "" {
		restore.ExtTS, err = parseTS(o.ts)
		if err != nil {
			return nil, err
		}
	}
	if o.conf != "" {
		var buf []byte
		if o.conf == "-" {
			buf, err = io.ReadAll(os.Stdin)
		} else {
			buf, err = os.ReadFile(o.conf)
		}
		if err != nil {
			return nil, errors.Wrap(err, "unable to read config file")
		}
		err = yaml.UnmarshalStrict(buf, &restore.ExtConf)
		if err != nil {
			return nil, errors.Wrap(err, "unable to  unmarshal config file")
		}
	}
	err = cn.SendCmd(pbm.Cmd{
		Cmd:     pbm.CmdRestore,
		Restore: restore,
	})
	if err != nil {
		return nil, errors.Wrap(err, "send command")
	}

	if outf != outText {
		return &pbm.RestoreMeta{
			Name:   restore.Name,
			Backup: o.bcp,
			Type:   bcpType,
		}, nil
	}

	bcpName := o.bcp
	if o.extern {
		bcpName = "[external]"
	}
	fmt.Printf("Starting restore %s from '%s'", restore.Name, bcpName)

	var (
		fn     getRestoreMetaFn
		ctx    context.Context
		cancel context.CancelFunc
	)

	// physical restore may take more time to start
	const waitPhysRestoreStart = time.Second * 120
	if bcpType == pbm.LogicalBackup {
		fn = cn.GetRestoreMeta
		ctx, cancel = context.WithTimeout(context.Background(), pbm.WaitActionStart)
	} else {
		ep, _ := cn.GetEpoch()
		stg, err := cn.GetStorage(cn.Logger().NewEvent(string(pbm.CmdRestore), o.bcp, "", ep.TS()))
		if err != nil {
			return nil, errors.Wrap(err, "get storage")
		}

		fn = func(name string) (*pbm.RestoreMeta, error) {
			return pbm.GetPhysRestoreMeta(name, stg, cn.Logger().NewEvent(string(pbm.CmdRestore), o.bcp, "", ep.TS()))
		}
		ctx, cancel = context.WithTimeout(context.Background(), waitPhysRestoreStart)
	}
	defer cancel()

	return waitForRestoreStatus(ctx, cn, restore.Name, fn)
}

func runFinishRestore(o descrRestoreOpts) (fmt.Stringer, error) {
	stg, err := getRestoreMetaStg(o.cfg)
	if err != nil {
		return nil, errors.Wrap(err, "get storage")
	}

	path := fmt.Sprintf("%s/%s/cluster", pbm.PhysRestoresDir, o.restore)
	return outMsg{"Command sent. Check `pbm describe-restore ...` for the result."},
		stg.Save(path+"."+string(pbm.StatusCopyDone),
			bytes.NewReader([]byte(
				fmt.Sprintf("%d", time.Now().Unix()),
			)), -1)
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

func pitrestore(cn *pbm.PBM, t, base string, nss []string, rsMap map[string]string, outf outFormat) (rmeta *pbm.RestoreMeta, err error) {
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
			Name:       name,
			TS:         int64(ts.T),
			I:          int64(ts.I),
			Bcp:        base,
			Namespaces: nss,
			RSMap:      rsMap,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "send command")
	}

	if outf != outText {
		return &pbm.RestoreMeta{Name: name}, nil
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
	Name               string           `json:"name" yaml:"name"`
	OPID               string           `json:"opid" yaml:"opid"`
	Backup             string           `json:"backup" yaml:"backup"`
	Type               pbm.BackupType   `json:"type" yaml:"type"`
	Status             pbm.Status       `json:"status" yaml:"status"`
	Error              *string          `json:"error,omitempty" yaml:"error,omitempty"`
	Namespaces         []string         `json:"namespaces,omitempty" yaml:"namespaces,omitempty"`
	StartTS            *int64           `json:"start_ts,omitempty" yaml:"-"`
	StartTime          *string          `json:"start,omitempty" yaml:"start,omitempty"`
	PITR               *int64           `json:"ts_to_restore,omitempty" yaml:"-"`
	PITRTime           *string          `json:"time_to_restore,omitempty" yaml:"time_to_restore,omitempty"`
	LastTransitionTS   int64            `json:"last_transition_ts" yaml:"-"`
	LastTransitionTime string           `json:"last_transition_time" yaml:"last_transition_time"`
	Replsets           []RestoreReplset `json:"replsets" yaml:"replsets"`
}

type RestoreReplset struct {
	Name               string        `json:"name" yaml:"name"`
	Status             pbm.Status    `json:"status" yaml:"status"`
	Error              *string       `json:"error,omitempty" yaml:"error,omitempty"`
	LastTransitionTS   int64         `json:"last_transition_ts" yaml:"-"`
	LastTransitionTime string        `json:"last_transition_time" yaml:"last_transition_time"`
	Nodes              []RestoreNode `json:"nodes,omitempty" yaml:"nodes,omitempty"`
}

type RestoreNode struct {
	Name               string     `json:"name" yaml:"name"`
	Status             pbm.Status `json:"status" yaml:"status"`
	Error              *string    `json:"error,omitempty" yaml:"error,omitempty"`
	LastTransitionTS   int64      `json:"last_transition_ts" yaml:"-"`
	LastTransitionTime string     `json:"last_transition_time" yaml:"last_transition_time"`
}

func (r describeRestoreResult) String() string {
	b, err := yaml.Marshal(r)
	if err != nil {
		return fmt.Sprintln("error:", err)
	}

	return string(b)
}

func getRestoreMetaStg(cfgPath string) (storage.Storage, error) {
	buf, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read config file")
	}

	var cfg pbm.Config
	err = yaml.UnmarshalStrict(buf, &cfg)
	if err != nil {
		return nil, errors.Wrap(err, "unable to  unmarshal config file")
	}

	l := log.New(nil, "cli", "").NewEvent("", "", "", primitive.Timestamp{})
	return pbm.Storage(cfg, l)
}

func describeRestore(cn *pbm.PBM, o descrRestoreOpts) (fmt.Stringer, error) {
	var (
		meta *pbm.RestoreMeta
		err  error
		res  describeRestoreResult
	)
	if o.cfg != "" {
		stg, err := getRestoreMetaStg(o.cfg)
		if err != nil {
			return nil, errors.Wrap(err, "get storage")
		}
		meta, err = pbm.GetPhysRestoreMeta(o.restore, stg, log.New(nil, "cli", "").NewEvent("", "", "", primitive.Timestamp{}))
		if err != nil && meta == nil {
			return nil, errors.Wrap(err, "get restore meta")
		}
	} else {
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
	res.Namespaces = meta.Namespaces
	res.OPID = meta.OPID
	res.LastTransitionTS = meta.LastTransitionTS
	res.LastTransitionTime = time.Unix(res.LastTransitionTS, 0).UTC().Format(time.RFC3339)
	if meta.Status == pbm.StatusError {
		res.Error = &meta.Error
	}
	if meta.StartPITR != 0 {
		res.StartTS = &meta.StartPITR
		s := time.Unix(meta.StartPITR, 0).UTC().Format(time.RFC3339)
		res.StartTime = &s
	}
	if meta.PITR != 0 {
		res.PITR = &meta.PITR
		s := time.Unix(meta.PITR, 0).UTC().Format(time.RFC3339)
		res.PITRTime = &s
	}

	for _, rs := range meta.Replsets {
		mrs := RestoreReplset{
			Name:               rs.Name,
			Status:             rs.Status,
			LastTransitionTS:   rs.LastTransitionTS,
			LastTransitionTime: time.Unix(rs.LastTransitionTS, 0).UTC().Format(time.RFC3339),
		}
		if rs.Status == pbm.StatusError {
			mrs.Error = &rs.Error
		}
		for _, node := range rs.Nodes {
			mnode := RestoreNode{
				Name:               node.Name,
				Status:             node.Status,
				LastTransitionTS:   node.LastTransitionTS,
				LastTransitionTime: time.Unix(node.LastTransitionTS, 0).UTC().Format(time.RFC3339),
			}
			if node.Status == pbm.StatusError {
				serr := node.Error
				mnode.Error = &serr
			}

			if rs.Status == pbm.StatusPartlyDone &&
				node.Status != pbm.StatusDone &&
				node.Status != pbm.StatusError {
				mnode.Status = pbm.StatusError
				serr := fmt.Sprintf("Node lost. Last heartbeat: %d", node.Hb.T)
				mnode.Error = &serr
			}

			mrs.Nodes = append(mrs.Nodes, mnode)
		}
		res.Replsets = append(res.Replsets, mrs)
	}

	return res, nil
}
