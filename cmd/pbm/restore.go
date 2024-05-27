package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mongodb/mongo-tools/common/db"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"gopkg.in/yaml.v2"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
	"github.com/percona/percona-backup-mongodb/pbm/util"
)

type restoreOpts struct {
	bcp           string
	pitr          string
	pitrBase      string
	wait          bool
	extern        bool
	ns            string
	usersAndRoles bool
	rsMap         string
	conf          string
	ts            string
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
		m := fmt.Sprintf("\nRestore finished! Check pbm describe-restore %s", r.Name)
		if r.physical {
			m += " -c </path/to/pbm.conf.yaml>\nRestart the cluster and pbm-agents, and run `pbm config --force-resync`"
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

func runRestore(ctx context.Context, conn connect.Client, o *restoreOpts, outf outFormat) (fmt.Stringer, error) {
	nss, err := parseCLINSOption(o.ns)
	if err != nil {
		return nil, errors.Wrap(err, "parse --ns option")
	}
	if err := validateRestoreUsersAndRoles(o.usersAndRoles, nss); err != nil {
		return nil, errors.Wrap(err, "parse --with-users-and-roles-option")
	}

	rsMap, err := parseRSNamesMapping(o.rsMap)
	if err != nil {
		return nil, errors.Wrap(err, "cannot parse replset mapping")
	}

	if o.pitr != "" && o.bcp != "" {
		return nil, errors.New("either a backup name or point in time should be set, non both together!")
	}

	clusterTime, err := topo.GetClusterTime(ctx, conn)
	if err != nil {
		return nil, errors.Wrap(err, "read cluster time")
	}
	tdiff := time.Now().Unix() - int64(clusterTime.T)

	m, err := doRestore(ctx, conn, o, nss, rsMap, outf)
	if err != nil {
		return nil, err
	}
	if o.extern && outf == outText {
		err = waitRestore(ctx, conn, m, defs.StatusCopyReady, tdiff)
		if err != nil {
			return nil, errors.Wrap(err, "waiting for the `copyReady` status")
		}

		return externRestoreRet{Name: m.Name, Snapshot: o.bcp}, nil
	}
	if !o.wait {
		return restoreRet{
			Name:     m.Name,
			Snapshot: o.bcp,
			physical: m.Type == defs.PhysicalBackup || m.Type == defs.IncrementalBackup,
		}, nil
	}

	typ := " logical restore.\nWaiting to finish"
	if m.Type == defs.PhysicalBackup {
		typ = " physical restore.\nWaiting to finish"
	}
	fmt.Printf("Started%s", typ)
	err = waitRestore(ctx, conn, m, defs.StatusDone, tdiff)
	if err == nil {
		return restoreRet{
			Name:     m.Name,
			done:     true,
			physical: m.Type == defs.PhysicalBackup || m.Type == defs.IncrementalBackup,
		}, nil
	}

	if errors.Is(err, restoreFailedError{}) {
		return restoreRet{err: err.Error()}, nil
	}
	return restoreRet{err: fmt.Sprintf("%s.\n Try to check logs on node %s", err.Error(), m.Leader)}, nil
}

// We rely on heartbeats in error detection in case of all nodes failed,
// comparing heartbeats with the current cluster time for logical restores.
// But for physical ones, the cluster by this time is down. So we compare with
// the wall time taking into account a time skew (wallTime - clusterTime) taken
// when the cluster time was still available.
func waitRestore(
	ctx context.Context,
	conn connect.Client,
	m *restore.RestoreMeta,
	status defs.Status,
	tskew int64,
) error {
	ep, _ := config.GetEpoch(ctx, conn)
	l := log.FromContext(ctx).
		NewEvent(string(ctrl.CmdRestore), m.Backup, m.OPID, ep.TS())
	stg, err := util.GetStorage(ctx, conn, l)
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()

	var rmeta *restore.RestoreMeta

	getMeta := restore.GetRestoreMeta
	if m.Type == defs.PhysicalBackup || m.Type == defs.IncrementalBackup {
		getMeta = func(_ context.Context, _ connect.Client, name string) (*restore.RestoreMeta, error) {
			return restore.GetPhysRestoreMeta(name, stg, l)
		}
	}

	var ctime uint32
	frameSec := defs.StaleFrameSec
	if m.Type != defs.LogicalBackup {
		frameSec = 60 * 3
	}
	for range tk.C {
		fmt.Print(".")
		rmeta, err = getMeta(ctx, conn, m.Name)
		if errors.Is(err, errors.ErrNotFound) {
			continue
		}
		if err != nil {
			return errors.Wrap(err, "get restore metadata")
		}

		switch rmeta.Status {
		case status, defs.StatusDone, defs.StatusPartlyDone:
			return nil
		case defs.StatusError:
			return restoreFailedError{fmt.Sprintf("operation failed with: %s", rmeta.Error)}
		}

		if m.Type == defs.LogicalBackup {
			clusterTime, err := topo.GetClusterTime(ctx, conn)
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

type restoreFailedError struct {
	string
}

func (e restoreFailedError) Error() string {
	return e.string
}

func (e restoreFailedError) Is(err error) bool {
	if err == nil {
		return false
	}

	_, ok := err.(restoreFailedError) //nolint:errorlint
	return ok
}

func checkBackup(
	ctx context.Context,
	conn connect.Client,
	o *restoreOpts,
	nss []string,
) (string, defs.BackupType, error) {
	if o.extern && o.bcp == "" {
		return "", defs.ExternalBackup, nil
	}

	b := o.bcp
	if o.pitr != "" && o.pitrBase != "" {
		b = o.pitrBase
	}

	var err error
	var bcp *backup.BackupMeta
	if b != "" {
		bcp, err = backup.NewDBManager(conn).GetBackupByName(ctx, b)
		if errors.Is(err, errors.ErrNotFound) {
			return "", "", errors.Errorf("backup '%s' not found", b)
		}
	} else {
		var ts primitive.Timestamp
		ts, err = parseTS(o.pitr)
		if err != nil {
			return "", "", errors.Wrap(err, "parse pitr")
		}

		bcp, err = backup.GetLastBackup(ctx, conn, &primitive.Timestamp{T: ts.T + 1, I: 0})
		if errors.Is(err, errors.ErrNotFound) {
			return "", "", errors.New("no base snapshot found")
		}
	}
	if err != nil {
		return "", "", errors.Wrap(err, "get backup data")
	}
	if len(nss) != 0 && bcp.Type != defs.LogicalBackup {
		return "", "", errors.New("--ns flag is only allowed for logical restore")
	}
	if bcp.Status != defs.StatusDone {
		return "", "", errors.Errorf("backup '%s' didn't finish successfully", b)
	}

	return bcp.Name, bcp.Type, nil
}

func doRestore(
	ctx context.Context,
	conn connect.Client,
	o *restoreOpts,
	nss []string,
	rsMapping map[string]string,
	outf outFormat,
) (*restore.RestoreMeta, error) {
	bcp, bcpType, err := checkBackup(ctx, conn, o, nss)
	if err != nil {
		return nil, err
	}
	err = checkConcurrentOp(ctx, conn)
	if err != nil {
		return nil, err
	}

	name := time.Now().UTC().Format(time.RFC3339Nano)

	cmd := ctrl.Cmd{
		Cmd: ctrl.CmdRestore,
		Restore: &ctrl.RestoreCmd{
			Name:          name,
			BackupName:    bcp,
			Namespaces:    nss,
			UsersAndRoles: o.usersAndRoles,
			RSMap:         rsMapping,
			External:      o.extern,
		},
	}
	if o.pitr != "" {
		cmd.Restore.OplogTS, err = parseTS(o.pitr)
		if err != nil {
			return nil, err
		}
	}

	if o.ts != "" {
		cmd.Restore.ExtTS, err = parseTS(o.ts)
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
		err = yaml.UnmarshalStrict(buf, &cmd.Restore.ExtConf)
		if err != nil {
			return nil, errors.Wrap(err, "unable to  unmarshal config file")
		}
	}

	err = sendCmd(ctx, conn, cmd)
	if err != nil {
		return nil, errors.Wrap(err, "send command")
	}

	if outf != outText {
		return &restore.RestoreMeta{
			Name:   name,
			Backup: bcp,
			Type:   bcpType,
		}, nil
	}

	bcpName := ""
	if bcp != "" {
		bcpName = fmt.Sprintf(" from '%s'", bcp)
	}
	if o.extern {
		bcpName = " from [external]"
	}
	pitrs := ""
	if o.pitr != "" {
		pitrs = fmt.Sprintf(" to point-in-time %s", o.pitr)
	}
	fmt.Printf("Starting restore %s%s%s", name, pitrs, bcpName)

	var (
		fn     getRestoreMetaFn
		cancel context.CancelFunc
	)

	// physical restore may take more time to start
	const waitPhysRestoreStart = time.Second * 120
	var startCtx context.Context
	if bcpType == defs.LogicalBackup {
		fn = restore.GetRestoreMeta
		startCtx, cancel = context.WithTimeout(ctx, defs.WaitActionStart)
	} else {
		ep, _ := config.GetEpoch(ctx, conn)
		logger := log.FromContext(ctx)
		l := logger.NewEvent(string(ctrl.CmdRestore), bcp, "", ep.TS())
		stg, err := util.GetStorage(ctx, conn, l)
		if err != nil {
			return nil, errors.Wrap(err, "get storage")
		}

		fn = func(_ context.Context, _ connect.Client, name string) (*restore.RestoreMeta, error) {
			return restore.GetPhysRestoreMeta(name, stg,
				logger.NewEvent(string(ctrl.CmdRestore), bcp, "", ep.TS()))
		}
		startCtx, cancel = context.WithTimeout(ctx, waitPhysRestoreStart)
	}
	defer cancel()

	return waitForRestoreStatus(startCtx, conn, name, fn)
}

func runFinishRestore(o descrRestoreOpts) (fmt.Stringer, error) {
	stg, err := getRestoreMetaStg(o.cfg)
	if err != nil {
		return nil, errors.Wrap(err, "get storage")
	}

	path := fmt.Sprintf("%s/%s/cluster", defs.PhysRestoresDir, o.restore)
	return outMsg{"Command sent. Check `pbm describe-restore ...` for the result."},
		stg.Save(path+"."+string(defs.StatusCopyDone),
			bytes.NewReader([]byte(
				fmt.Sprintf("%d", time.Now().Unix()),
			)), -1)
}

func parseTS(t string) (primitive.Timestamp, error) {
	var ts primitive.Timestamp
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

type getRestoreMetaFn func(ctx context.Context, conn connect.Client, name string) (*restore.RestoreMeta, error)

func waitForRestoreStatus(
	ctx context.Context,
	conn connect.Client,
	name string,
	getfn getRestoreMetaFn,
) (*restore.RestoreMeta, error) {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()

	meta := new(restore.RestoreMeta) // TODO
	for {
		select {
		case <-tk.C:
			fmt.Print(".")

			var err error
			meta, err = getfn(ctx, conn, name)
			if errors.Is(err, errors.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, errors.Wrap(err, "get metadata")
			}
			if meta == nil {
				continue
			}
			switch meta.Status {
			case defs.StatusRunning, defs.StatusDumpDone, defs.StatusDone:
				return meta, nil
			case defs.StatusError:
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
	Type               defs.BackupType  `json:"type" yaml:"type"`
	Status             defs.Status      `json:"status" yaml:"status"`
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
	Status             defs.Status   `json:"status" yaml:"status"`
	PartialTxn         []db.Oplog    `json:"partial_txn,omitempty" yaml:"-"`
	PartialTxnStr      *string       `json:"-" yaml:"partial_txn,omitempty"`
	LastTransitionTS   int64         `json:"last_transition_ts" yaml:"-"`
	LastTransitionTime string        `json:"last_transition_time" yaml:"last_transition_time"`
	Nodes              []RestoreNode `json:"nodes,omitempty" yaml:"nodes,omitempty"`
	Error              *string       `json:"error,omitempty" yaml:"error,omitempty"`
}

type RestoreNode struct {
	Name               string      `json:"name" yaml:"name"`
	Status             defs.Status `json:"status" yaml:"status"`
	Error              *string     `json:"error,omitempty" yaml:"error,omitempty"`
	LastTransitionTS   int64       `json:"last_transition_ts" yaml:"-"`
	LastTransitionTime string      `json:"last_transition_time" yaml:"last_transition_time"`
}

func (r describeRestoreResult) String() string {
	b, err := yaml.Marshal(r)
	if err != nil {
		return fmt.Sprintln("error:", err)
	}

	return string(b)
}

func getRestoreMetaStg(cfgPath string) (storage.Storage, error) {
	buf, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read config file")
	}

	var cfg config.Config
	err = yaml.UnmarshalStrict(buf, &cfg)
	if err != nil {
		return nil, errors.Wrap(err, "unable to  unmarshal config file")
	}

	l := log.New(nil, "cli", "").NewEvent("", "", "", primitive.Timestamp{})
	return util.StorageFromConfig(&cfg.Storage, l)
}

func describeRestore(ctx context.Context, conn connect.Client, o descrRestoreOpts) (fmt.Stringer, error) {
	var (
		meta *restore.RestoreMeta
		err  error
		res  describeRestoreResult
	)
	if o.cfg != "" {
		stg, err := getRestoreMetaStg(o.cfg)
		if err != nil {
			return nil, errors.Wrap(err, "get storage")
		}
		meta, err = restore.GetPhysRestoreMeta(o.restore, stg, log.New(nil, "cli", "").
			NewEvent("", "", "", primitive.Timestamp{}))
		if err != nil && meta == nil {
			return nil, errors.Wrap(err, "get restore meta")
		}
	} else {
		meta, err = restore.GetRestoreMeta(ctx, conn, o.restore)
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
	if meta.Status == defs.StatusError {
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
			PartialTxn:         rs.PartialTxn,
			LastTransitionTime: time.Unix(rs.LastTransitionTS, 0).UTC().Format(time.RFC3339),
		}
		if rs.Status == defs.StatusError {
			mrs.Error = &rs.Error
		} else if len(mrs.PartialTxn) > 0 {
			b, err := json.Marshal(mrs.PartialTxn)
			if err != nil {
				return res, errors.Wrap(err, "marshal partially committed transactions")
			}
			str := string(b)
			mrs.PartialTxnStr = &str
			perr := "WARNING! Some distributed transactions were not full in the oplog for this shard. " +
				"But were applied on other shard(s). See the list of not applied ops in `partial_txn`."
			mrs.Error = &perr
		}
		for _, node := range rs.Nodes {
			mnode := RestoreNode{
				Name:               node.Name,
				Status:             node.Status,
				LastTransitionTS:   node.LastTransitionTS,
				LastTransitionTime: time.Unix(node.LastTransitionTS, 0).UTC().Format(time.RFC3339),
			}
			if node.Status == defs.StatusError {
				serr := node.Error
				mnode.Error = &serr
			}

			if rs.Status == defs.StatusPartlyDone &&
				node.Status != defs.StatusDone &&
				node.Status != defs.StatusError {
				mnode.Status = defs.StatusError
				serr := fmt.Sprintf("Node lost. Last heartbeat: %d", node.Hb.T)
				mnode.Error = &serr
			}

			mrs.Nodes = append(mrs.Nodes, mnode)
		}
		res.Replsets = append(res.Replsets, mrs)
	}

	return res, nil
}

func validateRestoreUsersAndRoles(usersAndRoles bool, nss []string) error {
	if !util.IsSelective(nss) && usersAndRoles {
		return errors.New("Including users and roles are only allowed for selected database " +
			"(use --ns flag for selective backup)")
	}
	if len(nss) >= 1 && util.ContainsSpecifiedColl(nss) && usersAndRoles {
		return errors.New("Including users and roles are not allowed for specific collection. " +
			"Use --ns='db.*' to specify the whole database instead.")
	}

	return nil
}
