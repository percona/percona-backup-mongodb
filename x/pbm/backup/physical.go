package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/x/pbm/compress"
	"github.com/percona/percona-backup-mongodb/x/pbm/config"
	"github.com/percona/percona-backup-mongodb/x/pbm/connect"
	"github.com/percona/percona-backup-mongodb/x/pbm/defs"
	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
	"github.com/percona/percona-backup-mongodb/x/pbm/phasesync"
	"github.com/percona/percona-backup-mongodb/x/pbm/status"
	"github.com/percona/percona-backup-mongodb/x/pbm/storage"
	"github.com/percona/percona-backup-mongodb/x/pbm/storage/factory"
	"github.com/percona/percona-backup-mongodb/x/pbm/task"
	"github.com/percona/percona-backup-mongodb/x/pbm/topo"
	"github.com/percona/percona-backup-mongodb/x/pbm/version"
)

var ErrInvalidOptions = errors.New("invalid backup options")

type Options struct {
	Type             defs.BackupType          `json:"type"`
	Compression      compress.CompressionType `json:"compression,omitempty"`
	CompressionLevel *int                     `json:"compression_level,omitempty"`
	NumParallelFiles int32                    `json:"num_parallel_files,omitempty"`
	Profile          string                   `json:"profile,omitempty"`
}

func (o *Options) validate() error {
	if o.Type != defs.PhysicalBackup {
		return errors.Wrapf(ErrInvalidOptions,
			"backup type %q: only %q is supported", o.Type, defs.PhysicalBackup)
	}

	if o.Compression == "" {
		o.Compression = defaultCompression
	}
	if !compress.IsValidCompressionType(string(o.Compression)) {
		return errors.Wrapf(ErrInvalidOptions, "compression type %q", o.Compression)
	}

	switch {
	case o.NumParallelFiles < 0:
		return errors.Wrapf(ErrInvalidOptions,
			"number of parallel files %d: must be positive", o.NumParallelFiles)
	case o.NumParallelFiles == 0:
		o.NumParallelFiles = defaultNumParallelFiles
	}

	return nil
}

var phases = []phasesync.Phase{
	PhasePrepare,
	PhaseStarting,
	PhaseBackupCursor,
	PhaseBackupCursorExt,
	PhaseRunning,
	PhaseDone,
}

const (
	PhasePrepare         phasesync.Phase = "prepare"
	PhaseStarting        phasesync.Phase = "starting"
	PhaseBackupCursor    phasesync.Phase = "backupCursor"
	PhaseBackupCursorExt phasesync.Phase = "backupCursorExt"
	PhaseRunning         phasesync.Phase = "running"
	PhaseDone            phasesync.Phase = "done"
)

const (
	// groupSize is how many agents take part in a backup.
	// todo: read it from the backup task instead of hardcoding it.
	groupSize = 3

	// barrierTTL is how long after an agent stops renewing its lease the rest
	// of the group declares it lost.
	barrierTTL = 10 * time.Second

	// phasesPrefix is where the agents of a backup sync their phases.
	phasesPrefix = "/pbm/tasks/phases/"
)

const (
	// todo: get it from config
	backupBufferSize = 10 * 1024
)

const (
	// defaultCompression is what a backup is compressed with when the client
	// asks for no particular compression.
	defaultCompression = compress.CompressionTypeNone

	// defaultNumParallelFiles is how many files are uploaded at once
	// when the client doesn't ask for a particular number.
	defaultNumParallelFiles = 1
)

// PhysSvc is the physical backup service.
// It orchestrates a physical backup:
// - on the web API servier, it starts the backup cluster-wide,
// - every agent involved in backkup executes backup core logic from Run method.
type PhysSvc struct {
	ccDB    *clientv3.Client
	repo    *Repo
	agentID string

	nodeConn *mongo.Client
	leadConn connect.Client

	statusSvc *status.Svc
	configSvc *config.Svc
	composer  *task.Composer
}

// NewPhysSvc creates the physical backup service.
func NewPhysSvc(
	ccDB *clientv3.Client,
	repo *Repo,
	nodeConn *mongo.Client,
	leadConn connect.Client,
	statusSvc *status.Svc,
	configSvc *config.Svc,
	agentID string,
	composer *task.Composer,
) *PhysSvc {
	return &PhysSvc{
		ccDB:      ccDB,
		repo:      repo,
		nodeConn:  nodeConn,
		leadConn:  leadConn,
		statusSvc: statusSvc,
		configSvc: configSvc,
		agentID:   agentID,
		composer:  composer,
	}
}

// Start begins a new physical backup and returns as soon as the work is delegated.
// Backup itself runs on the agents.
func (s *PhysSvc) Start(ctx context.Context, opts Options) (*task.BackupTask, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}

	return s.composer.Backup(ctx, newBackupName(), opts)
}

// Run performs this agent's part (core) of the physical backup.
func (s *PhysSvc) Run(ctx context.Context, name string, isLeader bool, rawOpts json.RawMessage) error {
	log.Printf("running backup %s; agent: %s; leader: %t", name, s.agentID, isLeader)
	startTime := time.Now().UTC()

	opts := Options{}
	if err := json.Unmarshal(rawOpts, &opts); err != nil {
		return errors.Wrap(err, "unmarshal backup options")
	}

	agent, err := s.statusSvc.GetMember(s.agentID)
	if err != nil {
		return errors.Wrap(err, "get agent")
	}
	isSharded := agent.MongoInfo.Sharded

	b, err := phasesync.New(ctx, s.ccDB, phasesync.Options{
		Prefix: phasePrefix(name),
		ID:     s.agentID,
		Size:   groupSize,
		Phases: phases,
		TTL:    barrierTTL,
	})
	if err != nil {
		return errors.Wrap(err, "join phase barrier")
	}
	defer func() {
		if err := b.Close(); err != nil {
			log.Printf("backup %s: %s: leave phase barrier: %v", name, s.agentID, err)
		}
	}()

	logEndPhase, err := s.advance(ctx, b, name, PhasePrepare, isSharded)
	if err != nil {
		return err
	}

	// bcp.SetMongoVersion(a.brief.Version.VersionString)
	// bcp.SetTimeouts(cfg.Backup.Timeouts)

	cfgName := config.DefaultConfigName
	if opts.Profile != "" {
		cfgName = opts.Profile
	}

	cfg, err := s.configSvc.Get(ctx, cfgName)
	if err != nil {
		return errors.Wrapf(err, "get config %q", cfgName)
	}

	stg, err := factory.Create(&cfg.Storage)
	if err != nil {
		return errors.Wrap(err, "create storage")
	}
	log.Printf("backup %s: %s: storage %s at %s", name, s.agentID, cfg.Storage.Typ(), cfg.Storage.Path())

	// add backup meta doc for the cluster, so only leader will do it once
	balancer := topo.BalancerModeOff
	if isLeader {
		if isSharded {
			bs, err := topo.GetBalancerStatus(ctx, s.leadConn)
			if err != nil {
				return errors.Wrap(err, "get balancer status")
			}
			if bs.IsOn() {
				balancer = topo.BalancerModeOn
			}
		}
		err = s.initMeta(ctx, name, opts.Compression, balancer, startTime.Unix())
		if err != nil {
			return errors.Wrap(err, "init meta")
		}
		log.Printf("init backup meta")
	}

	logEndPhase()

	logEndPhase, err = s.advance(ctx, b, name, PhaseStarting, isSharded)
	if err != nil {
		return err
	}

	rsMeta := &BackupReplset{
		Name:        agent.MongoInfo.SetName,
		Node:        agent.MongoInfo.Me,
		PBMVersion:  version.Current().Version,
		IsConfigSvr: &agent.MongoInfo.ConfigSvr,
		// MongoVersion: b.mongoVersion,
		StartTS: time.Now().UTC().Unix(),
		// Status:
		// Conditions:   []Condition{},
		// FirstWriteTS: oplogTS, // unimportant for physical
	}

	isConfigShard, err := topo.HasConfigShard(ctx, s.leadConn)
	if err != nil {
		return errors.Wrap(err, "has configshard")
	}
	if isConfigShard {
		rsMeta.IsConfigShard = &isConfigShard
	}

	err = s.repo.UpdateRSMeta(ctx, name, rsMeta)
	if err != nil {
		return errors.Wrap(err, "add rs meta")
	}

	//
	// step: setting balancer in the original status in any case
	// defer
	// 	errd := topo.SetBalancerStatus(context.Background(), b.leadConn, topo.BalancerModeOn)

	// step: checking storage access
	// err = storage.HasReadAccess(ctx, stg)
	// if err != nil {
	// 	if !errors.Is(err, storage.ErrUninitialized) {
	// 		return errors.Wrap(err, "check read access")
	// 	}
	//
	// 	if inf.IsLeader() {
	// 		err = util.Initialize(ctx, stg)
	// 		if err != nil {
	// 			return errors.Wrap(err, "init storage")
	// 		}
	// 	}
	// }
	//

	if isLeader && isSharded && balancer == topo.BalancerModeOn {
		if err = s.stopBalancer(); err != nil {
			return err
		}
	}

	logEndPhase()

	logEndPhase, err = s.advance(ctx, b, name, PhaseBackupCursor, isSharded)
	if err != nil {
		return err
	}

	currOpts := bson.D{}
	cursor := NewBackupCursor(s.nodeConn, currOpts)
	defer cursor.Close()

	bcur, err := cursor.Data(ctx)
	if err != nil {
		return errors.Wrap(err, "get backup files")
	}

	log.Printf("backup cursor id: %s", bcur.Meta.ID)

	lwts, err := topo.GetLastWrite(ctx, s.nodeConn, true)
	if err != nil {
		return errors.Wrap(err, "get shard's last write ts")
	}

	defOpts := &topo.MongodOpts{}
	defOpts.Storage.WiredTiger.EngineConfig.JournalCompressor = "snappy"
	defOpts.Storage.WiredTiger.CollectionConfig.BlockCompressor = "snappy"
	defOpts.Storage.WiredTiger.IndexConfig.PrefixCompression = true

	mopts, err := topo.GetMongodOpts(ctx, s.nodeConn, defOpts)
	if err != nil {
		return errors.Wrap(err, "get mongod options")
	}
	err = topo.ExpandSecOptsWithEncAtRest(ctx, s.nodeConn, mopts.Security)
	if err != nil {
		return errors.Wrap(err, "get encryption at rest options")
	}

	rsMeta.MongodOpts = mopts
	// rsMeta.Status =
	rsMeta.FirstWriteTS = bcur.Meta.OplogEnd.TS
	rsMeta.LastWriteTS = lwts
	if cursor.CustomThisID != "" {
		// custom thisBackupName was used
		rsMeta.CustomThisID = cursor.CustomThisID
	}
	err = s.repo.UpdateRSMeta(ctx, name, rsMeta)
	if err != nil {
		return errors.Wrap(err, "update metadata")
	}
	logEndPhase()

	logEndPhase, err = s.advance(ctx, b, name, PhaseBackupCursorExt, isSharded)
	if err != nil {
		return err
	}
	fwTS, lwTS := s.resolveFirstLastWriteForCluster(ctx, name)

	if isLeader {
		// todo update meta
		_, _ = fwTS, lwTS
	}

	log.Printf("set journal up to %v", lwTS)

	jrnls, err := cursor.Journals(lwTS)
	if err != nil {
		return errors.Wrap(err, "get journal files")
	}

	data := bcur.Data
	stgb, err := getStorageBSON(bcur.Meta.DBpath)
	if err != nil {
		if !errors.Is(err, storage.ErrNotExist) {
			return errors.Wrap(err, "check storage.bson file")
		}
	} else {
		data = append(data, *stgb)
	}
	logEndPhase()

	logEndPhase, err = s.advance(ctx, b, name, PhaseRunning, isSharded)
	if err != nil {
		return err
	}

	err = s.uploadPhysical(ctx, name, opts, rsMeta, data, jrnls, bcur.Meta.DBpath, stg)
	if err != nil {
		return errors.Wrap(err, "upload")
	}
	logEndPhase()

	logEndPhase, err = s.advance(ctx, b, name, PhaseDone, isSharded)
	if err != nil {
		return err
	}

	finishTime := time.Now().UTC()
	if isLeader {
		if err = s.repo.SetFinishTime(ctx, name, finishTime.Unix()); err != nil {
			return errors.Wrap(err, "set backup finish time")
		}
		if err = s.writeMeta(ctx, name, stg); err != nil {
			return errors.Wrap(err, "write backup meta")
		}
	}

	log.Printf("backup finished: %s, start: %v, finish: %v, duration: %v",
		name, startTime.Format(time.RFC3339), finishTime.Format(time.RFC3339), finishTime.Sub(startTime))
	return nil
}

// advance moves this agent to phase and blocks until the rest of the group
// reaches it. It returns the function that reports how long the phase's work
// took, to be called once that work is done.
func (s *PhysSvc) advance(
	ctx context.Context,
	b *phasesync.Barrier,
	name string,
	phase phasesync.Phase,
	isSharded bool,
) (func(), error) {
	msg := "phase %s for backup %s on %s"
	if isSharded {
		msg = "phase %s for backup %s on %s, waiting for the group"
	}
	log.Printf(msg, phase, name, s.agentID)

	if err := b.Advance(ctx, phase); err != nil {
		return nil, errors.Wrapf(err, "advance to %q", phase)
	}
	ps := time.Now()

	return func() {
		log.Printf("backup %s: %s: %s phase took %s", name, s.agentID, phase, time.Since(ps))
	}, nil
}

func (s *PhysSvc) initMeta(
	ctx context.Context,
	name string,
	compression compress.CompressionType,
	balancer topo.BalancerMode,
	startTime int64,
) error {
	meta := &BackupMeta{
		Type:        defs.PhysicalBackup,
		Name:        name,
		Compression: compression,
		// Store: Storage{
		// 	Name:        b.config.Name,
		// 	IsProfile:   b.config.IsProfile,
		// 	StorageConf: b.config.Storage,
		// },
		StartTS:  time.Now().Unix(),
		Status:   defs.StatusStarting,
		Replsets: []BackupReplset{},
		// the driver (mongo?) sets TS to the current wall clock if TS was 0, so have to init with 1
		LastWriteTS:  bson.Timestamp{T: 1, I: 1},
		FirstWriteTS: bson.Timestamp{T: 1, I: 1},
		PBMVersion:   version.Current().Version,
		// MongoVersion:   b.mongoVersion,
		BalancerStatus: balancer,
		StartTime:      startTime,
	}

	fcv, err := version.GetFCV(ctx, s.nodeConn)
	if err != nil {
		return errors.Wrap(err, "get featureCompatibilityVersion")
	}
	meta.FCV = fcv

	// todo: shards remap data

	return s.repo.Insert(ctx, meta)
}

func (s *PhysSvc) stopBalancer() error {
	// t := b.timeouts.BalancerStop()
	// if t > 0 {
	// 	l.Debug("stopping balancer with timeout %s", t)
	// 	err = topo.StopBalancer(ctx, b.leadConn, t.Milliseconds())
	// } else {
	// 	l.Debug("stopping balancer")
	// 	err = topo.SetBalancerStatus(ctx, b.leadConn, topo.BalancerModeOff)
	// }
	// if err != nil {
	// 	return errors.Wrap(err, "set balancer OFF")
	// }
	//
	// l.Debug("waiting for balancer off")
	// bs := topo.WaitForBalancerDisabled(ctx, b.leadConn, time.Second*30, l)
	// if bs.IsDisabled() {
	// 	l.Debug("balancer is disabled")
	// } else {
	// 	l.Warning("balancer is not disabled: balancer mode: %s, in balancer round: %t",
	// 		bs.Mode, bs.InBalancerRound)
	// }
	return nil
}

func (s *PhysSvc) resolveFirstLastWriteForCluster(ctx context.Context, name string) (bson.Timestamp, bson.Timestamp) {
	// todo
	return bson.Timestamp{}, bson.Timestamp{}
}

// writeMeta dumps the backup metadata on the storage.
func (s *PhysSvc) writeMeta(ctx context.Context, name string, stg storage.Storage) error {
	meta, err := s.repo.Get(ctx, name)
	if err != nil {
		return errors.Wrap(err, "get backup meta")
	}

	data, err := json.MarshalIndent(meta, "", "\t")
	if err != nil {
		return errors.Wrap(err, "marshal meta")
	}

	fname := name + defs.MetadataFileSuffix
	err = stg.Save(fname, bytes.NewReader(data), storage.Size(int64(len(data))))
	if err != nil {
		return errors.Wrapf(err, "save %q", fname)
	}
	log.Printf("backup %s: %s: meta written to %q", name, s.agentID, fname)

	return nil
}

func (s *PhysSvc) uploadPhysical(
	ctx context.Context,
	name string,
	opts Options,
	rsMeta *BackupReplset,
	data,
	jrnls []File,
	dbpath string,
	stg storage.Storage,
) error {
	if opts.NumParallelFiles > 1 {
		log.Printf("uploading data (%d files in parallel)", opts.NumParallelFiles)
	} else {
		log.Printf("uploading data")
	}

	dataFiles, err := uploadFiles(
		ctx,
		data,
		name+"/"+rsMeta.Name,
		dbpath,
		opts.Type == defs.IncrementalBackup,
		stg,
		opts.Compression,
		opts.CompressionLevel,
		backupBufferSize,
		int(opts.NumParallelFiles),
	)
	if err != nil {
		return errors.Wrap(err, "upload data files")
	}
	log.Printf("uploading data done")

	log.Printf("uploading journals")
	ju, err := uploadFiles(
		ctx,
		jrnls,
		name+"/"+rsMeta.Name,
		dbpath,
		false,
		stg,
		opts.Compression,
		opts.CompressionLevel,
		backupBufferSize,
		int(opts.NumParallelFiles),
	)
	if err != nil {
		return errors.Wrap(err, "upload journal files")
	}
	log.Printf("uploading journals done")

	filelist := Filelist(dataFiles)
	filelist = append(filelist, ju...)

	size := int64(0)
	sizeUncompressed := int64(0)
	for _, f := range filelist {
		size += f.StgSize
		if f.StgSize != 0 {
			// maintain uncompressed size just for the files that have a disk footprint,
			// backup meta might contains files which are unchanged from the previous
			// inc/base backup
			sizeUncompressed += f.StgSizeUncompressed
		}
	}

	filelistPath := path.Join(name, rsMeta.Name, FilelistName)
	flSize, err := storage.Upload(ctx, filelist, stg, compress.CompressionTypeNone, nil, filelistPath)
	if err != nil {
		return errors.Wrapf(err, "upload filelist %q", filelistPath)
	}
	log.Printf("uploaded: %q %s", filelistPath, storage.PrettySize(flSize))

	totalSize := size + flSize
	totalUncompressed := sizeUncompressed + flSize
	log.Printf("totalSize=%d; totalSizeUncompressed=%d", totalSize, totalUncompressed)

	// err = IncBackupSize(
	// 	ctx,
	// 	b.leadConn,
	// 	bcp.Name,
	// 	totalSize,
	// 	&totalUncompressed,
	// )
	// if err != nil {
	// 	return errors.Wrap(err, "inc backup size")
	// }
	// err = SetBackupSizeForRS(
	// 	ctx,
	// 	b.leadConn,
	// 	bcp.Name,
	// 	rsMeta.Name,
	// 	totalSize,
	// 	totalUncompressed,
	// )
	// if err != nil {
	// 	return errors.Wrap(err, "set RS backup size")
	// }

	return nil
}

// newBackupName renders a backup name for the given time.
// phasePrefix is the namespace the agents of one backup sync their phases in.
func phasePrefix(backupName string) string {
	return phasesPrefix + backupName + "/"
}

func newBackupName() string {
	const BackupNameFormat = "2006-01-02T15:04:05Z"
	return time.Now().UTC().Format(BackupNameFormat)
}

// uploadFiles uploads the given files to the storage, running up concurrently
// `numWorkers` function calls of writeFile.
func uploadFiles(
	ctx context.Context,
	files []File,
	subdir string,
	trimPrefix string,
	incr bool,
	stg storage.Storage,
	comprT compress.CompressionType,
	comprL *int,
	bufSize int,
	numWorkers int,
) ([]File, error) {
	if len(files) == 0 {
		return nil, nil
	}

	upItems := planUploads(files, incr)

	// each concurrent upload needs its own set of buffer (x3)
	type uploadBufs struct{ cp, save, fsSave []byte }
	bufPool := make(chan uploadBufs, numWorkers)
	allBufs := make([]byte, numWorkers*3*bufSize)
	for i := range numWorkers {
		base := i * 3 * bufSize
		bufPool <- uploadBufs{
			cp:     allBufs[base : base+bufSize : base+bufSize],
			save:   allBufs[base+bufSize : base+2*bufSize : base+2*bufSize],
			fsSave: allBufs[base+2*bufSize : base+3*bufSize : base+3*bufSize],
		}
	}

	// each goroutine writes a distinct index, so no locking needed.
	results := make([]File, len(upItems))
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(min(numWorkers, len(upItems)))

	for i, s := range upItems {
		fname := trimFilePrefix(s.file.Name, trimPrefix)
		if !s.upload {
			s.file.Name = fname
			results[i] = s.file
			continue
		}

		// fail fast if single item fails
		if egCtx.Err() != nil {
			break
		}

		eg.Go(func() error {
			bufs := <-bufPool
			defer func() { bufPool <- bufs }()

			fw, err := writeFile(
				egCtx,
				&s.file,
				path.Join(subdir, fname),
				stg,
				comprT,
				comprL,
				bufs.cp,
				bufs.save,
				bufs.fsSave,
			)
			if err != nil {
				return errors.Wrapf(err, "upload file `%s`", s.file.Name)
			}
			fw.Name = fname

			results[i] = *fw
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return results, nil
}

func writeFile(
	ctx context.Context,
	file *File,
	dst string,
	stg storage.Storage,
	compression compress.CompressionType,
	compressLevel *int,
	cpBuf []byte,
	saveBuf []byte,
	fsSaveBuf []byte,
) (*File, error) {
	fstat, err := os.Stat(file.Name)
	if err != nil {
		return nil, errors.Wrap(err, "get file stat")
	}

	dst += compression.Suffix()
	sz := fstat.Size()
	if file.Len != 0 {
		// Len is always a multiple of the fixed size block (16Mb default)
		// so Off + Len might be bigger than the actual file size
		sz = file.Len
		if file.Off+file.Len > file.Size {
			sz = file.Size - file.Off
		}
		dst += fmt.Sprintf(".%d-%d", file.Off, file.Len)
	}

	var src storage.Source = file
	if len(cpBuf) > 0 {
		src = NewFileReader(*file, cpBuf)
	}
	_, err = storage.UploadWithOpts(ctx, src, stg, compression, compressLevel, dst,
		sz, saveBuf, fsSaveBuf)
	if err != nil {
		return nil, errors.Wrap(err, "upload file")
	}

	finf, err := stg.FileStat(dst)
	if err != nil {
		return nil, errors.Wrapf(err, "get storage file stat %s", dst)
	}

	return &File{
		Name:                file.Name,
		Size:                fstat.Size(),
		Fmode:               fstat.Mode(),
		StgSize:             finf.Size,
		StgSizeUncompressed: sz,
		Off:                 file.Off,
		Len:                 file.Len,
	}, nil
}

// upItem is one entry of an upload.
// It contains the file name and and info about whether it's necessary to upload it.
type upItem struct {
	file   File
	upload bool
}

// planUploads walks files in order and produces the upload plan. files may
// come as 16Mb (by default) blocks; in that case consecutive blocks of the
// same file are coalesced into one bigger upload. In [Off-Len] notation:
// f1[0-16], f1[16-16], f1[64-16] becomes f1[0-32], f1[64-16].
// If this is an incremental, NOT base backup, unchanged files (Len == 0) are
// not uploaded but still recorded in the meta as we need to know what files
// shouldn't be restored (those which aren't in the target backup).
func planUploads(files []File, incr bool) []upItem {
	if len(files) == 0 {
		return nil
	}

	upItems := make([]upItem, 0, len(files))
	wfile := files[0]
	for _, file := range files[1:] {
		// Skip uploading unchanged files if incremental
		// but add them to the meta to keep track of files to be restored
		// from prev backups. Plus sometimes the cursor can return an offset
		// beyond the current file size. Such phantom changes shouldn't
		// be copied. But save meta to have file size.
		if incr && (file.Len == 0 || file.Off >= file.Size) {
			file.Off = -1
			file.Len = -1

			upItems = append(upItems, upItem{file: file})
			continue
		}

		if wfile.Name == file.Name &&
			wfile.Off+wfile.Len == file.Off {
			wfile.Len += file.Len
			wfile.Size = file.Size
			continue
		}

		upItems = append(upItems, upItem{file: wfile, upload: true})
		wfile = file
	}

	// flush the last pending file unless it's an incremental no-op
	if !incr || wfile.Off != 0 || wfile.Len != 0 {
		upItems = append(upItems, upItem{file: wfile, upload: true})
	}

	return upItems
}
