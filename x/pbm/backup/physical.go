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
	PhaseStarting,
	PhaseMetaCreated,
	PhaseMetaRSCreated,
	PhaseBackupCursor,
	PhaseBackupCursorExt,
	PhaseDone,
}

const (
	PhaseStarting        phasesync.Phase = "starting"
	PhaseMetaCreated     phasesync.Phase = "metaCreated"
	PhaseMetaRSCreated   phasesync.Phase = "metaCreatedRS"
	PhaseBackupCursor    phasesync.Phase = "backupCursor"
	PhaseBackupCursorExt phasesync.Phase = "backupCursorExt"
	PhaseDone            phasesync.Phase = "done"
)

const (
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
// - on the web API server, it starts the backup cluster-wide,
// - every agent involved in backup executes backup core logic from Run method.
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
func (s *PhysSvc) Run(
	ctx context.Context,
	name string,
	groupSize int,
	isLeader bool,
	rawOpts json.RawMessage,
) (err error) {
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

	// todo: read this from status
	mVer, err := version.GetMongoVersion(ctx, s.nodeConn)
	if err != nil {
		return errors.Wrap(err, "mongodb version")
	}

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

	pTS, err := s.advance(ctx, b, name, PhaseStarting, isSharded, startTime)
	if err != nil {
		return err
	}

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
		err = s.initMeta(ctx, name, opts.Compression, balancer, startTime.Unix(), mVer)
		if err != nil {
			return errors.Wrap(err, "init meta")
		}
		log.Printf("init backup meta")
	}

	defer func() {
		if err != nil && isLeader {
			derr := s.repo.SetError(context.Background(), name, err)
			if derr != nil {
				log.Printf("error while setting cluster error status: %v", derr)
			}
		}
	}()

	pTS, err = s.advance(ctx, b, name, PhaseMetaCreated, isSharded, pTS)
	if err != nil {
		return err
	}

	rsMeta := &BackupReplset{
		Name:         agent.MongoInfo.SetName,
		Node:         agent.MongoInfo.Me,
		PBMVersion:   version.Current().Version,
		IsConfigSvr:  &agent.MongoInfo.ConfigSvr,
		MongoVersion: mVer.VersionString,
		Status:       StatusInProgress,
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
	defer func() {
		if err != nil {
			derr := s.repo.SetRSError(context.Background(), name, agent.MongoInfo.SetName, err)
			if derr != nil {
				log.Printf("error while setting RS error status: %v", derr)
			}
		}
	}()

	if isLeader && isSharded && balancer == topo.BalancerModeOn {
		if err = s.stopBalancer(ctx); err != nil {
			return errors.Wrap(err, "stop balancer")
		}
	}
	defer func() {
		if isSharded && balancer == topo.BalancerModeOn {
			errB := topo.SetBalancerStatus(context.Background(), s.leadConn, topo.BalancerModeOn)
			if errB != nil {
				// todo: log this with highest severity
				log.Printf("error while starting balancer: %s", errB)
			}
			log.Printf("balancer is on")
		}
	}()

	pTS, err = s.advance(ctx, b, name, PhaseMetaRSCreated, isSharded, pTS)
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

	pTS, err = s.advance(ctx, b, name, PhaseBackupCursor, isSharded, pTS)
	if err != nil {
		return err
	}

	fwTS, lwTS, err := s.resolveFirstLastWriteForCluster(ctx, name)
	if err != nil {
		return errors.Wrap(err, "resolve first and last write")
	}

	if isLeader {
		err = s.repo.SetFirstLastWrite(ctx, name, fwTS, lwTS)
		if err != nil {
			return errors.Wrap(err, "set meta for first and last write")
		}
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
	pTS, err = s.advance(ctx, b, name, PhaseBackupCursorExt, isSharded, pTS)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil && isLeader {
			// todo: delete backup files or add config rules to keep them
		}
	}()
	err = s.uploadPhysical(ctx, name, opts, rsMeta, data, jrnls, bcur.Meta.DBpath, stg)
	if err != nil {
		return errors.Wrap(err, "upload")
	}

	if err = s.repo.SetRSDone(ctx, name, agent.MongoInfo.SetName); err != nil {
		return errors.Wrap(err, "set RS done status")
	}

	_, err = s.advance(ctx, b, name, PhaseDone, isSharded, pTS)
	if err != nil {
		return err
	}

	finishTime := time.Now().UTC()
	if isLeader {
		if err = s.setClusterSize(ctx, name); err != nil {
			return errors.Wrap(err, "set backup size")
		}
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

// advance moves this agent to phase and blocks until the rest of the group reaches it.
// It adds duration logging info for sharded cluster and RS.
func (s *PhysSvc) advance(
	ctx context.Context,
	b *phasesync.Barrier,
	name string,
	phase phasesync.Phase,
	isSharded bool,
	phaseStartTime time.Time,
) (time.Time, error) {
	workingD := time.Now().UTC().Sub(phaseStartTime)
	msg := "phase: %s for backup %s on %s"
	if isSharded {
		msg = "phase: %s for backup %s on %s, waiting for the group"
	}
	log.Printf(msg, phase, name, s.agentID)

	if err := b.Advance(ctx, phase); err != nil {
		return time.Time{}, errors.Wrapf(err, "advance to %q", phase)
	}

	phaseD := time.Now().UTC().Sub(phaseStartTime)
	waitD := phaseD - workingD
	if isSharded {
		log.Printf("phase %s reached; working: %s, waiting: %s, total phase: %s duration",
			phase, workingD, waitD, phaseD)
	} else {
		log.Printf("phase %s reached; total phase: %s duration", phase, phaseD)
	}

	return time.Now().UTC(), nil
}

func (s *PhysSvc) initMeta(
	ctx context.Context,
	name string,
	compression compress.CompressionType,
	balancer topo.BalancerMode,
	startTime int64,
	mongoVer version.MongoVersion,
) error {
	meta := &BackupMeta{
		Type:        defs.PhysicalBackup,
		Name:        name,
		Compression: compression,
		Status:      StatusInProgress,
		Replsets:    []BackupReplset{},
		// the driver (mongo?) sets TS to the current wall clock if TS was 0, so have to init with 1
		LastWriteTS:    bson.Timestamp{T: 1, I: 1},
		FirstWriteTS:   bson.Timestamp{T: 1, I: 1},
		PBMVersion:     version.Current().Version,
		MongoVersion:   mongoVer.PSMDBVersion,
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

// stopBalancer turns the balancer off and waits for the running round to end.
func (s *PhysSvc) stopBalancer(ctx context.Context) error {
	const (
		// todo: wire to config
		timeout      = 30 * time.Minute
		pollInterval = 5 * time.Second
	)

	log.Printf("stopping balancer with timeout %s", timeout)
	if err := topo.StopBalancer(ctx, s.leadConn, timeout.Milliseconds()); err != nil {
		return errors.Wrap(err, "set balancer OFF")
	}

	log.Printf("waiting for balancer off")
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	tk := time.NewTicker(pollInterval)
	defer tk.Stop()

	// the last status read, to tell what the balancer was doing on timeout
	var last *topo.BalancerStatus
	for {
		bs, err := topo.GetBalancerStatus(waitCtx, s.leadConn)
		switch {
		case err != nil:
			log.Printf("get balancer status: %v", err)
		case bs.IsDisabled():
			log.Printf("balancer is disabled")
			return nil
		default:
			last = bs
		}

		select {
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if last == nil {
				log.Printf("balancer status is unknown after %s", timeout)
				return nil
			}
			log.Printf("balancer is not disabled: balancer mode: %s, in balancer round: %t",
				last.Mode, last.InBalancerRound)

			return nil

		case <-tk.C:
		}
	}
}

func (s *PhysSvc) resolveFirstLastWriteForCluster(
	ctx context.Context,
	name string,
) (bson.Timestamp, bson.Timestamp, error) {
	meta, err := s.repo.Get(ctx, name)
	if err != nil {
		return bson.Timestamp{}, bson.Timestamp{}, errors.Wrap(err, "meta for first and last write")
	}

	// todo: check this logic, it should be fixed here
	fw := meta.Replsets[0].FirstWriteTS
	lw := meta.Replsets[0].LastWriteTS
	for _, rs := range meta.Replsets {
		if rs.FirstWriteTS.After(fw) {
			fw = rs.FirstWriteTS
		}
		if rs.LastWriteTS.After(lw) {
			lw = rs.LastWriteTS
		}
	}

	return fw, lw, nil
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
	log.Printf("RS total size=%d; total size uncompressed=%d", totalSize, totalUncompressed)

	err = s.repo.SetRSSize(ctx, name, rsMeta.Name, totalSize, totalUncompressed)
	if err != nil {
		return errors.Wrap(err, "set RS backup size")
	}

	return nil
}

// setClusterSize sums up the sizes of all replsets and records them as the
// cluster-wide backup size.
func (s *PhysSvc) setClusterSize(ctx context.Context, name string) error {
	meta, err := s.repo.Get(ctx, name)
	if err != nil {
		return errors.Wrap(err, "get backup meta")
	}

	size := int64(0)
	sizeUncompressed := int64(0)
	for _, rs := range meta.Replsets {
		size += rs.Size
		sizeUncompressed += rs.SizeUncompressed
	}
	log.Printf("cluster total size=%d; total size uncompressed=%d", size, sizeUncompressed)

	return s.repo.SetSize(ctx, name, size, sizeUncompressed)
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
