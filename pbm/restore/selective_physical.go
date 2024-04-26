package restore

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"golang.org/x/mod/semver"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
	"github.com/percona/percona-backup-mongodb/pbm/util"
	"github.com/percona/percona-backup-mongodb/pbm/version"
)

const SelectivePhysicalHeartbeatInternal = 5 * time.Second

var wtBackupFiles = []string{
	"storage.bson",
	"WiredTiger",
	"WiredTiger.backup",
	"WiredTigerHS.wt",
	"_mdb_catalog.wt",
	"sizeStorer.wt",
}

type storageMetadata map[string]struct {
	Table string `bson:"tableMetadata"`
	File  string `bson:"fileMetadata"`
}

type wtMetadata struct {
	Any bson.M `bson:",inline"`

	StorageMetadata storageMetadata   `bson:"storageMetadata"`
	CollectionFile  string            `bson:"collectionFile"`
	IndexFiles      map[string]string `bson:"indexFiles"`

	Metadata struct {
		Any      bson.M            `bson:",inline"`
		NS       string            `bson:"ns"`
		IdxIdent map[string]string `bson:"idxIdent"`
		Ident    string            `bson:"ident"`
		Meta     struct {
			Any     bson.M `bson:",inline"`
			Options struct {
				Any  bson.M           `bson:",inline"`
				UUID primitive.Binary `bson:"uuid"`
			} `bson:"options"`
		} `bson:"md"`
	} `bson:"metadata"`
}

type SelectivePhysicalOptions struct {
	LeaderConn connect.Client
	NodeConn   *mongo.Client
	NodeInfo   *topo.NodeInfo
	Config     *config.Config
	Storage    storage.Storage
	Name       string
	Backup     *backup.BackupMeta
	Namespaces []string
}

type SelectivePhysical struct {
	stopHeartbeat func()

	config  *config.Config
	storage storage.Storage

	leader connect.Client
	node   *mongo.Client

	info   *topo.NodeInfo
	dbpath string

	name   string
	backup *backup.BackupMeta
	nss    []string
}

func NewSelectivePhysical(ctx context.Context, options SelectivePhysicalOptions) (*SelectivePhysical, error) {
	dbpath, err := getDBPath(ctx, options.NodeConn)
	if err != nil {
		return nil, errors.Wrap(err, "get dbpath")
	}

	rv := &SelectivePhysical{
		stopHeartbeat: func() {},

		config:  options.Config,
		storage: options.Storage,
		leader:  options.LeaderConn,
		node:    options.NodeConn,
		info:    options.NodeInfo,
		dbpath:  dbpath,
		name:    options.Name,
		backup:  options.Backup,
		nss:     options.Namespaces,
	}
	return rv, nil
}

func (r *SelectivePhysical) CloseWithError(ctx context.Context, cause error) error {
	r.stopHeartbeat()

	err := r.updateStatusImpl(ctx, defs.StatusError, cause)
	return errors.Wrap(err, "mark backup as failed")
}

func (r *SelectivePhysical) Done(ctx context.Context) error {
	r.stopHeartbeat()

	err := r.updateStatus(ctx, defs.StatusDone)
	return errors.Wrap(err, "mark backup as done")
}

func (r *SelectivePhysical) Init(ctx context.Context, opid ctrl.OPID) error {
	if r.backup.Status != defs.StatusDone {
		return errors.Errorf("backup is not done (actual status %q)", r.backup.Status)
	}
	if util.IsSelective(r.backup.Namespaces) {
		return errors.New("selective physical restore from selective backup is not supported")
	}

	if r.info.IsSharded() {
		return errors.New("selective physical restore for sharded cluster is not supported")
	}
	if len(r.info.Hosts) != 1 {
		return errors.New("selective physical restore for multi-member replset is not supported")
	}

	mongoVersion, err := version.GetMongoVersion(ctx, r.node)
	if err != nil || len(mongoVersion.Version) < 1 {
		return errors.Wrap(err, "define mongo version")
	}

	if semver.Compare(majmin(r.backup.MongoVersion), majmin(mongoVersion.VersionString)) != 0 {
		return errors.Errorf("backup mongod version (%q) is not the same as the running mongod version (%q)",
			r.backup.MongoVersion, mongoVersion.VersionString)
	}

	clusterTime, err := topo.GetClusterTime(ctx, r.leader)
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}

	nowUnix := time.Now().UTC().Unix()
	meta := &RestoreMeta{
		Type:    defs.LogicalBackup,
		OPID:    opid.String(),
		Name:    r.name,
		Backup:  r.backup.Name,
		StartTS: nowUnix,
		Status:  defs.StatusStarting,
		Replsets: []RestoreReplset{{
			Name:    r.info.SetName,
			StartTS: nowUnix,
			Status:  defs.StatusStarting,

			LastTransitionTS: nowUnix,
			Conditions: Conditions{{
				Timestamp: nowUnix,
				Status:    defs.StatusStarting,
			}},
		}},

		LastTransitionTS: nowUnix,
		Conditions: Conditions{{
			Timestamp: nowUnix,
			Status:    defs.StatusStarting,
		}},

		Hb: clusterTime,
	}

	_, err = r.leader.RestoresCollection().InsertOne(ctx, meta)
	if err != nil {
		return errors.Wrap(err, "write restore meta to db")
	}

	r.runHeartbeat(ctx)

	return nil
}

func (r *SelectivePhysical) runHeartbeat(ctx context.Context) {
	elog := log.LogEventFromContext(ctx)
	elog.Debug("running heartbeat")

	stopErr := errors.New("graceful stop")
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	r.stopHeartbeat = func() { cancel(stopErr) }

	go func() {
		for {
			if err := RestoreHB(ctx, r.leader, r.name); err != nil {
				if errors.Is(err, stopErr) ||
					errors.Is(err, context.Canceled) ||
					errors.Is(err, context.DeadlineExceeded) {
					elog.Debug("stopping heartbeat: %v", err)
					return
				}

				elog.Error("send heartbeat: %v", err)
			}

			time.Sleep(SelectivePhysicalHeartbeatInternal)
		}
	}()
}

func (r *SelectivePhysical) Run(ctx context.Context) error {
	err := r.updateStatus(ctx, defs.StatusRunning)
	if err != nil {
		return errors.Wrapf(err, "update status to %v", defs.StatusRunning)
	}
	filelist, err := r.readFilelist(ctx)
	if err != nil {
		return errors.Wrap(err, "read filelist")
	}
	filemap := make(map[string]backup.File, len(filelist))
	for _, file := range filelist {
		filemap[file.Name] = file
	}

	elog := log.LogEventFromContext(ctx)
	elog.Debug("reading wt metadata")
	md, err := r.readWTMetadata(ctx, filemap)
	if err != nil {
		return errors.Wrap(err, "read wiredtiger metadata")
	}

	elog.Debug("dropping namespaces")
	err = r.dropNamespaces(ctx)
	if err != nil {
		return errors.Wrap(err, "drop namespaces")
	}

	elog.Debug("importing collection")
	err = r.importCollection(ctx, filemap, md)
	if err != nil {
		return errors.Wrap(err, "import collection")
	}

	return nil
}

func (r *SelectivePhysical) updateStatus(ctx context.Context, status defs.Status) error {
	return r.updateStatusImpl(ctx, status, nil)
}

func (r *SelectivePhysical) updateStatusImpl(ctx context.Context, status defs.Status, cause error) error {
	errText := ""
	leaderErrText := ""
	if cause != nil {
		errText = cause.Error()
		leaderErrText = fmt.Sprintf("%s: %s: %s", r.info.SetName, r.info.Me, errText)
	}

	log.LogEventFromContext(ctx).Debug("updaing status: %v", status)
	nowUnix := time.Now().UTC().Unix()

	leaderUpdate := mongo.UpdateOneModel{
		Filter: bson.D{{"name", r.name}},
		Update: bson.D{
			{"$set", bson.M{"status": status}},
			{"$set", bson.M{"error": leaderErrText}},
			{"$set", bson.M{"last_transition_ts": nowUnix}},
			{"$push", bson.M{"conditions": Condition{
				Timestamp: nowUnix,
				Status:    status,
				Error:     leaderErrText,
			}}},
		},
	}

	rsUpdate := mongo.UpdateOneModel{
		Filter: bson.D{{"name", r.name}, {"replsets.name", r.info.SetName}},
		Update: bson.D{
			{"$set", bson.M{"replsets.$.status": status}},
			{"$set", bson.M{"replsets.$.error": errText}},
			{"$set", bson.M{"replsets.$.last_transition_ts": nowUnix}},
			{"$push", bson.M{"replsets.$.conditions": Condition{
				Timestamp: nowUnix,
				Status:    status,
				Error:     errText,
			}}},
		},
	}

	ops := []mongo.WriteModel{&leaderUpdate, &rsUpdate}
	_, err := r.leader.RestoresCollection().BulkWrite(ctx, ops)
	return err
}

func (r *SelectivePhysical) readWTMetadata(ctx context.Context, filemap map[string]backup.File) (*wtMetadata, error) {
	tempDBPath, err := os.MkdirTemp(r.dbpath, "pbm_*")
	if err != nil {
		return nil, errors.Wrap(err, "mktempdir")
	}
	defer func() {
		if err := os.RemoveAll(tempDBPath); err != nil {
			log.LogEventFromContext(ctx).
				Warning("readWTMetadata: unable to delete temp dir: %v", err)
		}
	}()

	eg, copyCtx := errgroup.WithContext(ctx)
	eg.SetLimit(runtime.GOMAXPROCS(0))

	for _, filename := range wtBackupFiles {
		eg.Go(func() error {
			file, ok := filemap[filename]
			if !ok {
				return errors.Errorf("missed file: %s", filename)
			}

			target := filepath.Join(tempDBPath, file.Name)
			err := r.copyFile(copyCtx, &file, target)
			return errors.Wrapf(err, "copy file %q => %q", file.Filename(r.backup.Compression), target)
		})
	}
	if err = eg.Wait(); err != nil {
		return nil, err
	}

	metadata, err := readWTMetadata(ctx, r.node, tempDBPath)
	if err != nil {
		return nil, err
	}

	isSelected := util.MakeSelectedPred(r.nss)
	for _, md := range metadata {
		if isSelected(md.Metadata.NS) {
			return md, nil
		}
	}

	return nil, errors.New("metadata not found")
}

func (r *SelectivePhysical) dropNamespaces(ctx context.Context) error {
	for _, ns := range r.nss {
		db, coll, _ := strings.Cut(ns, ".")
		err := dropCollection(ctx, r.node, db, coll)
		if err != nil {
			return errors.Wrapf(err, "drop %q", ns)
		}
	}

	return nil
}

//nolint:nonamedreturns
func (r *SelectivePhysical) importCollection(
	ctx context.Context,
	filemap map[string]backup.File,
	md *wtMetadata,
) (err error) {
	nsUUID, err := uuid.NewV7()
	if err != nil {
		return errors.Wrap(err, "new UUIDv7")
	}
	uuidData, _ := nsUUID.MarshalBinary()
	md.Metadata.Meta.Options.UUID.Data = uuidData

	newStgMeta := make(storageMetadata, len(md.StorageMetadata))
	newIdxFiles := make(map[string]string, len(md.IndexFiles))

	type copyFile struct {
		target string
		file   backup.File
	}

	n := fmt.Sprintf("%019d", rand.Uint64())
	files := make([]copyFile, 0, len(md.Metadata.IdxIdent)+1) // +1 for data file

	file, ok := filemap[md.Metadata.Ident+".wt"]
	if !ok {
		return errors.Errorf("missed data file: %s", md.Metadata.Ident+".wt")
	}

	newIdent := fmt.Sprintf("collection-0-%s", n)
	files = append(files, copyFile{newIdent + ".wt", file})

	newStgMeta[newIdent] = md.StorageMetadata[md.Metadata.Ident]
	md.CollectionFile = filepath.Join(r.dbpath, newIdent+".wt")
	md.Metadata.Ident = newIdent

	ord := 1
	for idxName, idxIdent := range md.Metadata.IdxIdent {
		file, ok := filemap[idxIdent+".wt"]
		if !ok {
			return errors.Errorf("missed index file: %s", idxIdent+".wt")
		}

		newIdxIdent := fmt.Sprintf("index-%d-%s", ord, n)
		files = append(files, copyFile{newIdxIdent + ".wt", file})

		newStgMeta[newIdxIdent] = md.StorageMetadata[idxIdent]
		newIdxFiles[newIdxIdent] = filepath.Join(r.dbpath, newIdxIdent+".wt")
		md.Metadata.IdxIdent[idxName] = newIdxIdent

		ord += 1
	}

	md.StorageMetadata = newStgMeta
	md.IndexFiles = newIdxFiles

	elog := log.LogEventFromContext(ctx)
	elog.Debug("checking if files exit")

	eg, _ := errgroup.WithContext(ctx)
	eg.SetLimit(runtime.GOMAXPROCS(0))

	for i := range files {
		eg.Go(func() error {
			filepath := filepath.Join(r.dbpath, files[i].target)

			yes, err := fileExists(filepath)
			if err != nil {
				return errors.Wrapf(err, "check if file exists: %s", filepath)
			}
			if yes {
				return errors.Errorf("file exists: %s", filepath)
			}

			return nil
		})
	}

	err = eg.Wait()
	if err != nil {
		return err
	}

	defer func() {
		if err == nil {
			return
		}

		for _, file := range files {
			filename := filepath.Join(r.dbpath, file.target)
			rmErr := r.removeFile(context.Background(), filename)
			if rmErr != nil {
				log.LogEventFromContext(ctx).
					Error("remove file %q: %v", filename, rmErr)
			}
		}
	}()

	elog.Debug("copying files")

	eg, copyCtx := errgroup.WithContext(ctx)
	eg.SetLimit(runtime.GOMAXPROCS(0))

	for _, file := range files {
		eg.Go(func() error {
			filename := filepath.Join(r.dbpath, file.target)
			err := r.copyFile(copyCtx, &file.file, filename)
			return errors.Wrapf(err, "copy file %q, ", file.file.Filename(r.backup.Compression))
		})
	}

	err = eg.Wait()
	if err != nil {
		return err
	}

	elog.Debug("attaching collection")
	return attachCollection(ctx, r.node, md)
}

//nolint:nonamedreturns
func (r *SelectivePhysical) copyFile(ctx context.Context, file *backup.File, targetFilename string) (err error) {
	elog := log.LogEventFromContext(ctx)
	defer func() {
		if err == nil {
			return
		}

		filename := filepath.Join(r.dbpath, file.Name)
		rmErr := r.removeFile(context.Background(), filename)
		if rmErr != nil {
			elog.Error("remove file %q: %v", filename, rmErr)
		}
	}()

	err = os.MkdirAll(filepath.Dir(targetFilename), os.ModeDir|0o700)
	if err != nil {
		return errors.Wrapf(err, "mkdir dir %q", filepath.Dir(targetFilename))
	}

	sourceFilename := path.Join(r.backup.Name, r.info.SetName, file.Filename(r.backup.Compression))
	compressedSrc, err := r.storage.SourceReader(sourceFilename)
	if err != nil {
		return errors.Wrap(err, "open storage file")
	}
	defer func() {
		if err := compressedSrc.Close(); err != nil {
			elog.Warning("copyFile: defer storage.SourceReader(%q): %v", sourceFilename, err)
		}
	}()

	src, err := compress.Decompress(compressedSrc, r.backup.Compression)
	if err != nil {
		return errors.Wrap(err, "open storage file")
	}
	defer func() {
		if err := src.Close(); err != nil {
			elog.Warning("copyFile: defer compress.Decompress(%q, %q): %v",
				sourceFilename, r.backup.Compression, err)
		}
	}()

	dst, err := os.OpenFile(targetFilename, os.O_CREATE|os.O_WRONLY, file.Fmode)
	if err != nil {
		return errors.Wrap(err, "create target file")
	}
	defer func() {
		if err := dst.Close(); err != nil {
			elog.Warning("copyFile: defer os.OpenFile(%q, %#o): %v", targetFilename, file.Fmode, err)
		}
	}()

	_, err = dst.Seek(file.Off, io.SeekStart)
	if err != nil {
		return errors.Wrap(err, "fseek")
	}

	_, err = io.Copy(dst, src)
	if err != nil {
		return errors.Wrap(err, "copy")
	}

	err = dst.Truncate(file.Size)
	if err != nil {
		return errors.Wrap(err, "truncate")
	}

	elog.Info("copied %q => %q", file.Name, targetFilename)
	return nil
}

func (r *SelectivePhysical) removeFile(_ context.Context, filename string) error {
	return os.Remove(filename)
}

//nolint:unused
func getCheckedRestoreMeta(ctx context.Context, c connect.Client, name string) (*RestoreMeta, error) {
	meta, err := GetRestoreMeta(ctx, c, name)
	if err != nil {
		return nil, err
	}

	clusterTime, err := topo.GetClusterTime(ctx, c)
	if err != nil {
		return nil, errors.Wrap(err, "read cluster time")
	}

	if meta.Hb.T+defs.StaleFrameSec < clusterTime.T {
		return nil, errors.Errorf("restore stuck (last beat %d)", meta.Hb.T)
	}

	if meta.Status == defs.StatusError {
		return nil, errors.Errorf("restore failed: %s", meta.Error)
	}

	return meta, nil
}

//nolint:unused
func (r *SelectivePhysical) waitForStatus(ctx context.Context, status defs.Status) error {
	for {
		meta, err := getCheckedRestoreMeta(ctx, r.leader, r.name)
		if err != nil {
			return errors.Wrap(err, "get restore meta")
		}

		if meta.Conditions.Has(status) {
			return nil
		}

		time.Sleep(5 * time.Second)
	}
}

func (r *SelectivePhysical) readFilelist(ctx context.Context) (backup.Filelist, error) {
	filelistPath := fmt.Sprintf("%s/%s/%s", r.backup.Name, r.info.SetName, backup.FilelistName)
	file, err := r.storage.SourceReader(filelistPath)
	if err != nil {
		return nil, errors.Wrapf(err, "open filelist %q", filelistPath)
	}
	defer func() {
		err := file.Close()
		if err != nil {
			log.LogEventFromContext(ctx).
				Warning("readFilelist: close: %v", err)
		}
	}()

	filelist, err := backup.ReadFilelist(file)
	if err != nil {
		return nil, errors.Wrap(err, "parse filelist")
	}

	return filelist, nil
}

func getDBPath(ctx context.Context, m *mongo.Client) (string, error) {
	opts, err := topo.GetMongodOpts(ctx, m, nil)
	if err != nil {
		return "", errors.Wrap(err, "get mongo options")
	}

	path := opts.Storage.DBpath
	if path == "" {
		path = defaultRSdbpath
	}

	return path, nil
}

func readWTMetadata(ctx context.Context, m *mongo.Client, path string) ([]*wtMetadata, error) {
	cur, err := m.Database("admin").Aggregate(ctx,
		mongo.Pipeline{{{"$backupMetadata", bson.D{{"backupPath", path}}}}})
	if err != nil {
		return nil, errors.Wrap(err, "pipeline: $backupMetadata")
	}
	defer func() {
		err := cur.Close(context.Background())
		if err != nil {
			l := log.LogEventFromContext(ctx)
			l.Warning("close $backupMetadata cursor: %v", err)
		}
	}()

	rv := []*wtMetadata{}
	for cur.Next(ctx) {
		w := &wtMetadata{}

		err = cur.Decode(&w)
		if err != nil {
			return nil, errors.Wrap(err, "decode")
		}

		rv = append(rv, w)
	}

	err = cur.Err()
	if err != nil {
		return nil, errors.Wrap(err, "cursor")
	}

	return rv, nil
}

func dropCollection(ctx context.Context, m *mongo.Client, db, coll string) error {
	opts := options.Database().SetWriteConcern(writeconcern.Journaled())
	res := m.Database(db, opts).RunCommand(ctx, bson.D{{"drop", coll}})
	return errors.Wrap(res.Err(), "cmd: drop")
}

func attachCollection(ctx context.Context, m *mongo.Client, md *wtMetadata) error {
	res := m.Database("admin").RunCommand(ctx, bson.D{{"attachCollection", md}})
	return errors.Wrap(res.Err(), "cmd: attachCollection")
}

func fileExists(name string) (bool, error) {
	_, err := os.Stat(name)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, errors.Wrap(err, "stat: if not exists")
	}

	return true, nil
}
