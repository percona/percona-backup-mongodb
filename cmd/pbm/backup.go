package main

import (
	"context"
	"fmt"
	stdlog "log"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/mod/semver"
	"gopkg.in/yaml.v2"

	"github.com/percona/percona-backup-mongodb/pbm/archive"
	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
	"github.com/percona/percona-backup-mongodb/pbm/util"
	"github.com/percona/percona-backup-mongodb/pbm/version"
	"github.com/percona/percona-backup-mongodb/sdk"
)

type backupOpts struct {
	name             string
	typ              string
	base             bool
	compression      string
	compressionLevel []int
	ns               string
	wait             bool
	externList       bool
}

type backupOut struct {
	Name    string `json:"name"`
	Storage string `json:"storage"`
}

func (b backupOut) String() string {
	return fmt.Sprintf("Backup '%s' to remote store '%s' has started", b.Name, b.Storage)
}

type externBcpOut struct {
	Name  string          `json:"name"`
	Nodes []externBcpNode `json:"storage"`

	list bool
}

type externBcpNode struct {
	Name  string   `json:"name"`
	Files []string `json:"files"`
}

func (b externBcpOut) String() string {
	s := fmt.Sprintln("Ready to copy data from:")
	for _, n := range b.Nodes {
		s += fmt.Sprintf("\t- %s\n", n.Name)
		if b.list {
			for _, f := range n.Files {
				s += fmt.Sprintf("\t\t%s\n", f)
			}
		}
	}
	s += fmt.Sprintf("After the copy is done, run: pbm backup-finish %s\n", b.Name)
	return s
}

type descBcp struct {
	name string
	coll bool
}

func runBackup(
	ctx context.Context,
	conn connect.Client,
	pbm sdk.Client,
	b *backupOpts,
	outf outFormat,
) (fmt.Stringer, error) {
	nss, err := parseCLINSOption(b.ns)
	if err != nil {
		return nil, errors.Wrap(err, "parse --ns option")
	}
	if len(nss) > 1 {
		return nil, errors.New("parse --ns option: multiple namespaces are not supported")
	}
	if len(nss) != 0 && b.typ != string(defs.LogicalBackup) {
		return nil, errors.New("--ns flag is only allowed for logical backup")
	}

	if err := topo.CheckTopoForBackup(ctx, conn, defs.BackupType(b.typ)); err != nil {
		return nil, errors.Wrap(err, "backup pre-check")
	}

	if err := checkConcurrentOp(ctx, conn); err != nil {
		// PITR slicing can be run along with the backup start - agents will resolve it.
		var e concurentOpError
		if !errors.As(err, &e) {
			return nil, err
		}
		if e.op.Type != ctrl.CmdPITR {
			return nil, err
		}
	}

	cfg, err := pbm.GetConfig(ctx)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errors.New("no store set. Set remote store with <pbm store set>")
		}
		return nil, errors.Wrap(err, "get remote-store")
	}

	compression := cfg.Backup.Compression
	if b.compression != "" {
		compression = compress.CompressionType(b.compression)
	}

	level := cfg.Backup.CompressionLevel
	if len(b.compressionLevel) != 0 {
		level = &b.compressionLevel[0]
	}

	err = sendCmd(ctx, conn, ctrl.Cmd{
		Cmd: ctrl.CmdBackup,
		Backup: &ctrl.BackupCmd{
			Type:             defs.BackupType(b.typ),
			IncrBase:         b.base,
			Name:             b.name,
			Namespaces:       nss,
			Compression:      compression,
			CompressionLevel: level,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "send command")
	}

	if outf != outText {
		return backupOut{b.name, cfg.Storage.Path()}, nil
	}

	fmt.Printf("Starting backup '%s'", b.name)
	startCtx, cancel := context.WithTimeout(ctx, cfg.Backup.Timeouts.StartingStatus())
	defer cancel()
	err = waitForBcpStatus(startCtx, conn, b.name)
	if err != nil {
		return nil, err
	}

	if b.typ == string(defs.ExternalBackup) {
		s, err := waitBackup(ctx, conn, b.name, defs.StatusCopyReady)
		if err != nil {
			return nil, errors.Wrap(err, "waiting for the `copyReady` status")
		}
		if s == nil || *s != defs.StatusCopyReady {
			str := "nil"
			if s != nil {
				str = string(*s)
			}
			return nil, errors.Errorf("unexpected backup status %v", str)
		}

		bcp, err := pbm.GetBackupByName(ctx, b.name, sdk.GetBackupByNameOptions{})
		if err != nil {
			return nil, errors.Wrap(err, "get backup meta")
		}
		out := externBcpOut{Name: b.name, list: b.externList}
		for _, rs := range bcp.Replsets {
			node := externBcpNode{Name: rs.Node}
			for _, f := range rs.Files {
				node.Files = append(node.Files, f.Name)
			}
			out.Nodes = append(out.Nodes, node)
		}
		return out, nil
	}

	if b.wait {
		fmt.Printf("\nWaiting for '%s' backup...", b.name)
		s, err := waitBackup(ctx, conn, b.name, defs.StatusDone)
		if s != nil {
			fmt.Printf(" %s\n", *s)
		}
		return outMsg{}, err
	}

	return backupOut{b.name, cfg.Storage.Path()}, nil
}

func runFinishBcp(ctx context.Context, conn connect.Client, bcp string) (fmt.Stringer, error) {
	meta, err := backup.NewDBManager(conn).GetBackupByName(ctx, bcp)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			return nil, errors.Errorf("backup %q not found", bcp)
		}
		return nil, err
	}
	if meta.Status != defs.StatusCopyReady {
		return nil, errors.Errorf("expected %q status. got %q", defs.StatusCopyReady, meta.Status)
	}

	return outMsg{fmt.Sprintf("Command sent. Check `pbm describe-backup %s` for the result.", bcp)},
		backup.ChangeBackupState(conn, bcp, defs.StatusCopyDone, "")
}

func waitBackup(ctx context.Context, conn connect.Client, name string, status defs.Status) (*defs.Status, error) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.C:
			bcp, err := backup.NewDBManager(conn).GetBackupByName(ctx, name)
			if err != nil {
				return nil, err
			}

			switch bcp.Status {
			case status, defs.StatusDone, defs.StatusCancelled:
				return &bcp.Status, nil
			case defs.StatusError:
				return &bcp.Status, bcp.Error()
			}
		}

		fmt.Print(".")
	}
}

func waitForBcpStatus(ctx context.Context, conn connect.Client, bcpName string) error {
	tk := time.NewTicker(time.Second)
	defer tk.Stop()

	var bmeta *backup.BackupMeta
	for {
		select {
		case <-tk.C:
			fmt.Print(".")
			var err error
			bmeta, err = backup.NewDBManager(conn).GetBackupByName(ctx, bcpName)
			if errors.Is(err, errors.ErrNotFound) {
				continue
			}
			if err != nil {
				return errors.Wrap(err, "get backup metadata")
			}
			switch bmeta.Status {
			case defs.StatusRunning, defs.StatusDumpDone, defs.StatusDone, defs.StatusCancelled:
				return nil
			case defs.StatusError:
				rs := ""
				for _, s := range bmeta.Replsets {
					rs += fmt.Sprintf("\n- Backup on replicaset \"%s\" in state: %v", s.Name, s.Status)
					if s.Error != "" {
						rs += ": " + s.Error
					}
				}
				return errors.New(bmeta.Error().Error() + rs)
			}
		case <-ctx.Done():
			if bmeta == nil {
				return errors.New("no progress from leader, backup metadata not found")
			}
			rs := ""
			for _, s := range bmeta.Replsets {
				rs += fmt.Sprintf("- Backup on replicaset \"%s\" in state: %v\n", s.Name, s.Status)
				if s.Error != "" {
					rs += ": " + s.Error
				}
			}
			if rs == "" {
				rs = "<no replset has started backup>\n"
			}

			return errors.New("no confirmation that backup has successfully started. Replsets status:\n" + rs)
		}
	}
}

type bcpDesc struct {
	Name               string          `json:"name" yaml:"name"`
	OPID               string          `json:"opid" yaml:"opid"`
	Type               defs.BackupType `json:"type" yaml:"type"`
	LastWriteTS        int64           `json:"last_write_ts" yaml:"-"`
	LastTransitionTS   int64           `json:"last_transition_ts" yaml:"-"`
	LastWriteTime      string          `json:"last_write_time" yaml:"last_write_time"`
	LastTransitionTime string          `json:"last_transition_time" yaml:"last_transition_time"`
	Namespaces         []string        `json:"namespaces,omitempty" yaml:"namespaces,omitempty"`
	MongoVersion       string          `json:"mongodb_version" yaml:"mongodb_version"`
	FCV                string          `json:"fcv" yaml:"fcv"`
	PBMVersion         string          `json:"pbm_version" yaml:"pbm_version"`
	Status             defs.Status     `json:"status" yaml:"status"`
	Size               int64           `json:"size" yaml:"-"`
	HSize              string          `json:"size_h" yaml:"size_h"`
	Err                *string         `json:"error,omitempty" yaml:"error,omitempty"`
	Replsets           []bcpReplDesc   `json:"replsets" yaml:"replsets"`
}

type bcpReplDesc struct {
	Name               string              `json:"name" yaml:"name"`
	Status             defs.Status         `json:"status" yaml:"status"`
	Node               string              `json:"node" yaml:"node"`
	Files              []backup.File       `json:"files,omitempty" yaml:"-"`
	LastWriteTS        int64               `json:"last_write_ts" yaml:"-"`
	LastTransitionTS   int64               `json:"last_transition_ts" yaml:"-"`
	LastWriteTime      string              `json:"last_write_time" yaml:"last_write_time"`
	LastTransitionTime string              `json:"last_transition_time" yaml:"last_transition_time"`
	IsConfigSvr        *bool               `json:"configsvr,omitempty" yaml:"configsvr,omitempty"`
	SecurityOpts       *topo.MongodOptsSec `json:"security,omitempty" yaml:"security,omitempty"`
	Error              *string             `json:"error,omitempty" yaml:"error,omitempty"`
	Collections        []string            `json:"collections,omitempty" yaml:"collections,omitempty"`
}

func (b *bcpDesc) String() string {
	data, err := yaml.Marshal(b)
	if err != nil {
		stdlog.Fatal(err)
	}

	return string(data)
}

func byteCountIEC(b int64) string {
	const unit = 1024

	if b < unit {
		return fmt.Sprintf("%d B", b)
	}

	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func describeBackup(ctx context.Context, conn connect.Client, pbm sdk.Client, b *descBcp) (fmt.Stringer, error) {
	bcp, err := pbm.GetBackupByName(ctx, b.name, sdk.GetBackupByNameOptions{})
	if err != nil {
		return nil, err
	}

	var stg storage.Storage
	if b.coll {
		l := log.FromContext(ctx).NewDefaultEvent()
		stg, err := util.GetStorage(ctx, conn, l)
		if err != nil {
			return nil, errors.Wrap(err, "get storage")
		}

		_, err = stg.FileStat(defs.StorInitFile)
		if err != nil {
			return nil, errors.Wrap(err, "check storage access")
		}
	}

	rv := &bcpDesc{
		Name:               bcp.Name,
		OPID:               bcp.OPID,
		Type:               bcp.Type,
		Namespaces:         bcp.Namespaces,
		MongoVersion:       bcp.MongoVersion,
		FCV:                bcp.FCV,
		PBMVersion:         bcp.PBMVersion,
		LastWriteTS:        int64(bcp.LastWriteTS.T),
		LastTransitionTS:   bcp.LastTransitionTS,
		LastWriteTime:      time.Unix(int64(bcp.LastWriteTS.T), 0).UTC().Format(time.RFC3339),
		LastTransitionTime: time.Unix(bcp.LastTransitionTS, 0).UTC().Format(time.RFC3339),
		Status:             bcp.Status,
		Size:               bcp.Size,
		HSize:              byteCountIEC(bcp.Size),
	}
	if bcp.Err != "" {
		rv.Err = &bcp.Err
	}

	if bcp.Size == 0 {
		switch bcp.Status {
		case defs.StatusDone, defs.StatusCancelled, defs.StatusError:
			l := log.FromContext(ctx).NewDefaultEvent()
			stg, err := util.GetStorage(ctx, conn, l)
			if err != nil {
				return nil, errors.Wrap(err, "get storage")
			}

			rv.Size, err = getLegacySnapshotSize(bcp, stg)
			if errors.Is(err, errMissedFile) && bcp.Status != defs.StatusDone {
				// canceled/failed backup can be incomplete. ignore
				return nil, errors.Wrap(err, "get snapshot size")
			}
		}
	}

	rv.Replsets = make([]bcpReplDesc, len(bcp.Replsets))
	for i, r := range bcp.Replsets {
		rv.Replsets[i] = bcpReplDesc{
			Name:               r.Name,
			Node:               r.Node,
			IsConfigSvr:        r.IsConfigSvr,
			Status:             r.Status,
			LastWriteTS:        int64(r.LastWriteTS.T),
			LastTransitionTS:   r.LastTransitionTS,
			LastWriteTime:      time.Unix(int64(r.LastWriteTS.T), 0).UTC().Format(time.RFC3339),
			LastTransitionTime: time.Unix(r.LastTransitionTS, 0).UTC().Format(time.RFC3339),
		}
		if r.Error != "" {
			e := r.Error
			rv.Replsets[i].Error = &e
		}
		if r.MongodOpts != nil && r.MongodOpts.Security != nil {
			rv.Replsets[i].SecurityOpts = r.MongodOpts.Security
		}
		if bcp.Type == defs.ExternalBackup {
			rv.Replsets[i].Files = r.Files
		}

		if !b.coll || bcp.Type != defs.LogicalBackup {
			continue
		}

		nss, err := backup.ReadArchiveNamespaces(stg, r.DumpName)
		if err != nil {
			return nil, errors.Wrap(err, "read archive metadata")
		}

		rv.Replsets[i].Collections = make([]string, len(nss))
		for j, ns := range nss {
			rv.Replsets[i].Collections[j] = archive.NSify(ns.Database, ns.Collection)
		}

		sort.Strings(rv.Replsets[i].Collections)
	}

	return rv, nil
}

// bcpsMatchCluster checks if given backups match shards in the cluster. Match means that
// each replset in backup has a respective replset on the target cluster. It's ok if cluster
// has more shards than there are currently in backup. But in the case of sharded cluster
// backup has to have data for the current config server or for the sole RS in case of non-sharded rs.
//
// If some backup doesn't match cluster, the status of the backup meta in given `bcps` would be
// changed to pbm.StatusError with respective error text emitted. It doesn't change meta on
// storage nor in DB (backup is ok, it just doesn't cluster), it is just "in-flight" changes
// in given `bcps`.
func bcpsMatchCluster(
	bcps []backup.BackupMeta,
	ver,
	fcv string,
	shards []topo.Shard,
	confsrv string,
	rsMap map[string]string,
) {
	sh := make(map[string]bool, len(shards))
	for _, s := range shards {
		sh[s.RS] = s.RS == confsrv
	}

	mapRS, mapRevRS := util.MakeRSMapFunc(rsMap), util.MakeReverseRSMapFunc(rsMap)
	for i := 0; i < len(bcps); i++ {
		bcpMatchCluster(&bcps[i], ver, fcv, sh, mapRS, mapRevRS)
	}
}

func bcpMatchCluster(bcp *backup.BackupMeta, ver, fcv string, shards map[string]bool, mapRS, mapRevRS util.RSMapFunc) {
	if bcp.Status != defs.StatusDone {
		return
	}
	if !version.CompatibleWith(bcp.PBMVersion, version.BreakingChangesMap[bcp.Type]) {
		bcp.SetRuntimeError(incompatiblePBMVersionError{bcp.PBMVersion})
		return
	}
	if bcp.FCV != "" {
		if bcp.FCV != fcv {
			bcp.SetRuntimeError(incompatibleFCVVersionError{bcp.FCV, fcv})
			return
		}
	} else if majmin(bcp.MongoVersion) != majmin(ver) {
		bcp.SetRuntimeError(incompatibleMongodVersionError{bcp.MongoVersion, ver})
		return

	}

	var nomatch []string
	hasconfsrv := false
	for i := range bcp.Replsets {
		name := mapRS(bcp.Replsets[i].Name)

		isconfsrv, ok := shards[name]
		if !ok {
			nomatch = append(nomatch, name)
		} else if mapRevRS(name) != bcp.Replsets[i].Name {
			nomatch = append(nomatch, name)
		}

		if isconfsrv {
			hasconfsrv = true
		}
	}

	if len(nomatch) != 0 || !hasconfsrv {
		names := make([]string, len(nomatch))
		copy(names, nomatch)
		bcp.SetRuntimeError(missedReplsetsError{names: names, configsrv: !hasconfsrv})
	}
}

func majmin(v string) string {
	if len(v) == 0 {
		return v
	}

	if v[0] != 'v' {
		v = "v" + v
	}

	return semver.MajorMinor(v)
}

var errIncompatible = errors.New("incompatible")

type missedReplsetsError struct {
	names     []string
	configsrv bool
}

func (e missedReplsetsError) Error() string {
	errString := ""
	if len(e.names) != 0 {
		errString = "Backup doesn't match current cluster topology - it has different replica set names. " +
			"Extra shards in the backup will cause this, for a simple example. " +
			"The extra/unknown replica set names found in the backup are: " + strings.Join(e.names, ", ")
	}

	if e.configsrv {
		if errString != "" {
			errString += ". "
		}
		errString += "Backup has no data for the config server or sole replicaset"
	}

	return errString
}

func (missedReplsetsError) Is(err error) bool {
	if err == nil {
		return false
	}

	_, ok := err.(missedReplsetsError) //nolint:errorlint
	return ok
}

func (missedReplsetsError) Unwrap() error {
	return errIncompatible
}

type incompatiblePBMVersionError struct {
	bcpVer string
}

func (e incompatiblePBMVersionError) Error() string {
	return fmt.Sprintf("backup version (v%s) is not compatible with PBM v%s",
		e.bcpVer, version.Current().Version)
}

func (incompatiblePBMVersionError) Is(err error) bool {
	if err == nil {
		return false
	}

	_, ok := err.(incompatiblePBMVersionError) //nolint:errorlint
	return ok
}

func (e incompatiblePBMVersionError) Unwrap() error {
	return errIncompatible
}

type incompatibleFCVVersionError struct {
	bcpVer  string
	currVer string
}

func (e incompatibleFCVVersionError) Error() string {
	return fmt.Sprintf("backup FCV %q is incompatible with the running mongo FCV %q", e.bcpVer, e.currVer)
}

func (incompatibleFCVVersionError) Is(err error) bool {
	if err == nil {
		return false
	}

	_, ok := err.(incompatibleFCVVersionError) //nolint:errorlint
	return ok
}

func (e incompatibleFCVVersionError) Unwrap() error {
	return errIncompatible
}

type incompatibleMongodVersionError struct {
	bcpVer  string
	currVer string
}

func (e incompatibleMongodVersionError) Error() string {
	return fmt.Sprintf(
		"backup mongo version %q is incompatible with the running mongo version %q",
		majmin(e.bcpVer), majmin(e.currVer))
}

func (incompatibleMongodVersionError) Is(err error) bool {
	if err == nil {
		return false
	}

	_, ok := err.(incompatibleMongodVersionError) //nolint:errorlint
	return ok
}

func (e incompatibleMongodVersionError) Unwrap() error {
	return errIncompatible
}
