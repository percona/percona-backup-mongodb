package restore

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/mongodb/mongo-tools-common/db"
	"github.com/mongodb/mongo-tools-common/options"
	"github.com/mongodb/mongo-tools/mongorestore"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

var excludeFromRestore = []string{
	pbm.DB + "." + pbm.CmdStreamCollection,
	pbm.DB + "." + pbm.LogCollection,
	pbm.DB + "." + pbm.ConfigCollection,
	pbm.DB + "." + pbm.BcpCollection,
	pbm.DB + "." + pbm.RestoresCollection,
	pbm.DB + "." + pbm.LockCollection,
	"config.version",
	"config.mongos",
	"config.lockpings",
	"config.locks",
	"config.system.sessions",
	"config.cache.*",
}

type Restore struct {
	name       string
	cn         *pbm.PBM
	node       *pbm.Node
	stopHB     chan struct{}
	nodeInfo   *pbm.IsMaster
	stg        storage.Storage
	bcp        *pbm.BackupMeta
	dumpFile   string
	oplogFile  string
	pitrChunks []pbm.PITRChunk
	pitrLastTS int64
	mgoV       *pbm.MongoVersion
}

// New creates a new restore object
func New(cn *pbm.PBM, node *pbm.Node) *Restore {
	return &Restore{
		cn:   cn,
		node: node,
	}
}

// Close releases object resources.
// Should be run to avoid leaks.
func (r *Restore) Close() {
	if r.stopHB != nil {
		close(r.stopHB)
	}
}

func (r *Restore) Snapshot(cmd pbm.RestoreCmd) (err error) {
	defer func() {
		if err != nil {
			ferr := r.MarkFailed(err)
			log.Printf("Error: mark restore as failed `%v`: %v\n", err, ferr)
		}

		r.Close()
	}()

	err = r.Init(cmd.Name)
	if err != nil {
		return err
	}

	err = r.PrepareBackup(cmd.BackupName)
	if err != nil {
		return err
	}

	err = r.RunSnapshot()
	if err != nil {
		return err
	}

	return r.Done()
}

func (r *Restore) PITR(cmd pbm.PITRestoreCmd) (err error) {
	defer func() {
		if err != nil {
			ferr := r.MarkFailed(err)
			log.Printf("Error: mark restore as failed `%v`: %v\n", err, ferr)
		}

		r.Close()
	}()

	err = r.Init(time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return err
	}

	err = r.PreparePITR(cmd.TS)
	if err != nil {
		return err
	}

	err = r.RunSnapshot()
	if err != nil {
		return err
	}

	err = r.RestoreChunks()
	if err != nil {
		return err
	}

	return r.Done()
}

func (r *Restore) Init(name string) (err error) {
	r.nodeInfo, err = r.node.GetIsMaster()
	if err != nil {
		return errors.Wrap(err, "get node data")
	}
	if r.nodeInfo.SetName == "" {
		return errors.Wrap(err, "unable to define replica set")
	}

	r.name = name

	meta := &pbm.RestoreMeta{
		Name:     r.name,
		StartTS:  time.Now().Unix(),
		Status:   pbm.StatusStarting,
		Replsets: []pbm.RestoreReplset{},
	}
	if r.nodeInfo.IsLeader() {
		err = r.cn.SetRestoreMeta(meta)
		if err != nil {
			return errors.Wrap(err, "write backup meta to db")
		}
		r.stopHB = make(chan struct{})
		go func() {
			tk := time.NewTicker(time.Second * 5)
			defer tk.Stop()
			for {
				select {
				case <-tk.C:
					err := r.cn.RestoreHB(r.name)
					if err != nil {
						log.Println("[ERROR] send pbm heartbeat:", err)
					}
				case <-r.stopHB:
					return
				}
			}
		}()
	}

	// Waiting for StatusStarting to move further.
	// In case some preparations has to be done before the restore.
	err = r.waitForStatus(pbm.StatusStarting)
	if err != nil {
		return errors.Wrap(err, "waiting for start")
	}

	rsMeta := pbm.RestoreReplset{
		Name:       r.nodeInfo.SetName,
		StartTS:    time.Now().UTC().Unix(),
		Status:     pbm.StatusStarting,
		Conditions: []pbm.Condition{},
	}

	defer func() {
		if err != nil {
			ferr := r.MarkFailed(err)
			log.Printf("Error: mark restore as failed `%v`: %v\n", err, ferr)
		}
	}()

	err = r.cn.AddRestoreRSMeta(r.name, rsMeta)
	if err != nil {
		return errors.Wrap(err, "add shard's metadata")
	}

	r.stg, err = r.cn.GetStorage()
	if err != nil {
		return errors.Wrap(err, "get backup storage")
	}

	r.mgoV, err = r.node.GetMongoVersion()
	if err != nil || len(r.mgoV.Version) < 1 {
		return errors.Wrap(err, "define mongo version")
	}

	return nil
}

func (r *Restore) PreparePITR(ts int64) (err error) {
	r.pitrLastTS = ts

	err = r.cn.SetRestorePITR(r.nodeInfo.SetName, ts)
	if err != nil {
		return errors.Wrap(err, "set PITR timestamp")
	}

	pts := primitive.Timestamp{T: uint32(ts), I: 0}

	lastChunk, err := r.cn.PITRGetChunkContains(r.nodeInfo.SetName, pts)
	if err != nil {
		return errors.Wrap(err, "define last oplog slice")
	}

	r.bcp, err = r.cn.GetLastBackup(&lastChunk.EndTS)

	err = r.prepareChunks(r.bcp.LastWriteTS, lastChunk.StartTS)
	if err != nil {
		return errors.Wrap(err, "verify oplog slices chain")
	}

	return r.prepareSnapshot()
}

// prepareChunks ensures integrity of oplog slices (timeline is continuous)
// and chunks exists on the storage
func (r *Restore) prepareChunks(from, to primitive.Timestamp) error {
	chunks, err := r.cn.PITRGetChunksSlice(r.nodeInfo.SetName, from, to)
	if err != nil {
		return errors.Wrap(err, "get chunks index")
	}

	nextStart := from
	for _, c := range chunks {
		if primitive.CompareTimestamp(nextStart, c.StartTS) != 0 {
			return errors.Errorf("integrity vilolated, expect chunk with start_ts %v, but got %v", nextStart, c.StartTS)
		}
		nextStart = c.EndTS

		err := r.stg.CheckFile(c.FName)
		if err != nil {
			return errors.Errorf("failed to ensure chunk %v.%v on the storage, file: %s, error: %v", c.StartTS, c.EndTS, c.FName, err)
		}
	}

	r.pitrChunks = chunks

	return nil
}

func (r *Restore) PrepareBackup(backupName string) (err error) {
	r.bcp, err = r.cn.GetBackupMeta(backupName)
	if errors.Cause(err) == mongo.ErrNoDocuments {
		r.bcp, err = r.getMetaFromStore(backupName)
	}

	if err != nil {
		return errors.Wrap(err, "get backup metadata")
	}

	err = r.prepareSnapshot()
	return errors.Wrap(err, "prepare snapshot")
}

func (r *Restore) prepareSnapshot() (err error) {
	if r.bcp == nil {
		return errors.New("snapshot name doesn't set")
	}

	err = r.cn.SetRestoreBackup(r.name, r.bcp.Name)
	if err != nil {
		return errors.Wrap(err, "set backup name")
	}

	if r.bcp.Status != pbm.StatusDone {
		return errors.Errorf("backup wasn't successful: status: %s, error: %s", r.bcp.Status, r.bcp.Error)
	}

	var ok bool
	for _, v := range r.bcp.Replsets {
		if v.Name == r.nodeInfo.SetName {
			r.dumpFile = v.DumpName
			r.oplogFile = v.OplogName
			ok = true
			break
		}
	}
	if !ok {
		return errors.Errorf("metadata for replset/shard %s is not found", r.nodeInfo.SetName)
	}

	err = r.stg.CheckFile(r.dumpFile)
	if err != nil {
		return errors.Errorf("failed to ensure snapshot file %s: %v", r.dumpFile, err)
	}
	err = r.stg.CheckFile(r.oplogFile)
	if err != nil {
		return errors.Errorf("failed to ensure oplog file %s: %v", r.oplogFile, err)
	}

	err = r.cn.ChangeRestoreRSState(r.name, r.nodeInfo.SetName, pbm.StatusRunning, "")
	if err != nil {
		return errors.Wrap(err, "set shard's StatusDumpDone")
	}

	return nil
}

const (
	tmpUsers = `pbmRUsers`
	tmpRoles = `pbmRRoles`

	preserveUUID = false
)

func (r *Restore) RunSnapshot() (err error) {
	err = r.cn.ChangeRestoreRSState(r.name, r.nodeInfo.SetName, pbm.StatusRunning, "")
	if err != nil {
		return errors.Wrap(err, "set shard's StatusDumpDone")
	}

	if r.nodeInfo.IsLeader() {
		err = r.reconcileStatus(pbm.StatusRunning, &pbm.WaitActionStart)
		if err != nil {
			if errors.Cause(err) == errConvergeTimeOut {
				return errors.Wrap(err, "couldn't get response from all shards")
			}
			return errors.Wrap(err, "check cluster for restore started")
		}
	}

	err = r.waitForStatus(pbm.StatusRunning)
	if err != nil {
		return errors.Wrap(err, "waiting for start")
	}

	sr, err := r.stg.SourceReader(r.dumpFile)
	if err != nil {
		return errors.Wrapf(err, "get object %s for the storage", r.dumpFile)
	}
	defer sr.Close()

	dumpReader, err := Decompress(sr, r.bcp.Compression)
	if err != nil {
		return errors.Wrapf(err, "decompress object %s", r.dumpFile)
	}
	defer dumpReader.Close()

	topts := options.ToolOptions{
		AppName:    "mongodump",
		VersionStr: "0.0.1",
		URI:        &options.URI{ConnectionString: r.node.ConnURI()},
		Auth:       &options.Auth{},
		Namespace:  &options.Namespace{},
		Connection: &options.Connection{},
		Direct:     true,
	}

	rsession, err := db.NewSessionProvider(topts)
	if err != nil {
		return errors.Wrap(err, "create session for the dump restore")
	}

	mr := mongorestore.MongoRestore{
		SessionProvider: rsession,
		ToolOptions:     &topts,
		InputOptions: &mongorestore.InputOptions{
			Archive: "-",
		},
		OutputOptions: &mongorestore.OutputOptions{
			BulkBufferSize:           2000,
			BypassDocumentValidation: true,
			Drop:                     true,
			NumInsertionWorkers:      20,
			NumParallelCollections:   1,
			PreserveUUID:             preserveUUID,
			StopOnError:              true,
			TempRolesColl:            "temproles",
			TempUsersColl:            "tempusers",
			WriteConcern:             "majority",
		},
		NSOptions: &mongorestore.NSOptions{
			NSExclude: excludeFromRestore,
			NSFrom:    []string{`admin.system.users`, `admin.system.roles`},
			NSTo:      []string{pbm.DB + `.` + tmpUsers, pbm.DB + `.` + tmpRoles},
		},
		InputReader: dumpReader,
	}

	rdumpResult := mr.Restore()
	if rdumpResult.Err != nil {
		return errors.Wrapf(rdumpResult.Err, "restore mongo dump (successes: %d / fails: %d)", rdumpResult.Successes, rdumpResult.Failures)
	}
	mr.Close()

	err = r.cn.ChangeRestoreRSState(r.name, r.nodeInfo.SetName, pbm.StatusDumpDone, "")
	if err != nil {
		return errors.Wrap(err, "set shard's StatusDumpDone")
	}
	log.Println("[INFO] mongorestore finished")

	if r.nodeInfo.IsLeader() {
		err = r.reconcileStatus(pbm.StatusDumpDone, nil)
		if err != nil {
			return errors.Wrap(err, "check cluster for restore dump done")
		}
	}

	err = r.waitForStatus(pbm.StatusDumpDone)
	if err != nil {
		return errors.Wrap(err, "waiting for start")
	}

	log.Println("[INFO] starting the oplog replay")

	or, err := r.stg.SourceReader(r.oplogFile)
	if err != nil {
		return errors.Wrapf(err, "get object %s for the storage", r.oplogFile)
	}
	defer or.Close()

	oplogReader, err := Decompress(or, r.bcp.Compression)
	if err != nil {
		return errors.Wrapf(err, "decompress object %s", r.oplogFile)
	}
	defer oplogReader.Close()

	err = NewOplog(r.node, r.mgoV, preserveUUID).Apply(oplogReader)
	if err != nil {
		return errors.Wrap(err, "oplog apply")
	}

	cusr, err := r.node.CurrentUser()
	if err != nil {
		return errors.Wrap(err, "get current user")
	}
	log.Println("[INFO] oplog replay finished")

	log.Println("restoring users and roles")
	err = r.restoreUsers(cusr)
	if err != nil {
		return errors.Wrap(err, "restore users 'n' roles")
	}

	return nil
}

func (r *Restore) RestoreChunks() error {
	log.Println("[INFO] replay pitr chunks")

	var upto int64
	for i, chnk := range r.pitrChunks {
		if i == len(r.pitrChunks)-1 {
			upto = r.pitrLastTS
		}
		err := r.replyChunk(chnk.FName, chnk.Compression, upto)
		if err != nil {
			return errors.Errorf("replay chunk %v.%v: %v", chnk.StartTS.T, chnk.EndTS.T, err)
		}
	}

	log.Println("[INFO] oplog replay finished")
	return nil
}

// !!! TODO: handle last chunk
func (r *Restore) replyChunk(file string, c pbm.CompressionType, upto int64) error {
	or, err := r.stg.SourceReader(file)
	if err != nil {
		return errors.Wrapf(err, "get object %s form the storage", file)
	}
	defer or.Close()

	oplogReader, err := Decompress(or, c)
	if err != nil {
		return errors.Wrapf(err, "decompress object %s", file)
	}
	defer oplogReader.Close()

	err = NewOplog(r.node, r.mgoV, preserveUUID).Apply(oplogReader)

	return errors.Wrap(err, "apply oplog for chunk")
}

func (r *Restore) Done() error {
	err := r.cn.ChangeRestoreRSState(r.name, r.nodeInfo.SetName, pbm.StatusDone, "")
	if err != nil {
		return errors.Wrap(err, "set shard's StatusDone")
	}

	if r.nodeInfo.IsLeader() {
		err = r.reconcileStatus(pbm.StatusDone, nil)
		if err != nil {
			return errors.Wrap(err, "check cluster for the restore done")
		}
	}

	return nil
}

func (r *Restore) swapUsers(ctx context.Context, exclude *pbm.AuthInfo) error {
	rolesC := r.node.Session().Database("admin").Collection("system.roles")

	eroles := []string{}
	for _, r := range exclude.UserRoles {
		eroles = append(eroles, r.DB+"."+r.Role)
	}

	curr, err := r.node.Session().Database(pbm.DB).Collection(tmpRoles).Find(ctx, bson.M{"_id": bson.M{"$nin": eroles}})
	if err != nil {
		return errors.Wrap(err, "create cursor for tmpRoles")
	}
	defer curr.Close(ctx)
	_, err = rolesC.DeleteMany(ctx, bson.M{"_id": bson.M{"$nin": eroles}})
	if err != nil {
		return errors.Wrap(err, "delete current roles")
	}

	for curr.Next(ctx) {
		rl := new(interface{})
		err := curr.Decode(rl)
		if err != nil {
			return errors.Wrap(err, "decode role")
		}
		_, err = rolesC.InsertOne(ctx, rl)
		if err != nil {
			return errors.Wrap(err, "insert role")
		}
	}

	user := ""
	if len(exclude.Users) > 0 {
		user = exclude.Users[0].DB + "." + exclude.Users[0].User
	}
	cur, err := r.node.Session().Database(pbm.DB).Collection(tmpUsers).Find(ctx, bson.M{"_id": bson.M{"$ne": user}})
	if err != nil {
		return errors.Wrap(err, "create cursor for tmpUsers")
	}
	defer cur.Close(ctx)

	log.Println("deleting users")
	usersC := r.node.Session().Database("admin").Collection("system.users")
	_, err = usersC.DeleteMany(ctx, bson.M{"_id": bson.M{"$ne": user}})
	if err != nil {
		return errors.Wrap(err, "delete current users")
	}

	log.Println("inserting users")
	for cur.Next(ctx) {
		u := new(interface{})
		err := cur.Decode(u)
		if err != nil {
			return errors.Wrap(err, "decode user")
		}
		_, err = usersC.InsertOne(ctx, u)
		if err != nil {
			return errors.Wrap(err, "insert user")
		}
		log.Println("inserted user")
	}

	err = r.node.Session().Database(pbm.DB).Collection(tmpRoles).Drop(ctx)
	if err != nil {
		return errors.Wrapf(err, "drop tmp roles collection %s", tmpRoles)
	}
	err = r.node.Session().Database(pbm.DB).Collection(tmpUsers).Drop(ctx)
	if err != nil {
		return errors.Wrapf(err, "drop tmp users collection %s", tmpUsers)
	}

	return nil
}

func (r *Restore) restoreUsers(exclude *pbm.AuthInfo) error {
	return r.swapUsers(r.cn.Context(), exclude)
}

func (r *Restore) reconcileStatus(status pbm.Status, timeout *time.Duration) error {
	shards := []pbm.Shard{
		{
			ID:   r.nodeInfo.SetName,
			Host: r.nodeInfo.SetName + "/" + strings.Join(r.nodeInfo.Hosts, ","),
		},
	}

	if r.nodeInfo.IsSharded() {
		s, err := r.cn.GetShards()
		if err != nil {
			return errors.Wrap(err, "get shards list")
		}
		shards = append(shards, s...)
	}

	if timeout != nil {
		return errors.Wrap(r.convergeClusterWithTimeout(shards, status, *timeout), "convergeClusterWithTimeout")
	}
	return errors.Wrap(r.convergeCluster(shards, status), "convergeCluster")
}

// convergeCluster waits until all given shards reached `status` and updates a cluster status
func (r *Restore) convergeCluster(shards []pbm.Shard, status pbm.Status) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	for {
		select {
		case <-tk.C:
			ok, err := r.converged(shards, status)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
		case <-r.cn.Context().Done():
			return nil
		}
	}
}

var errConvergeTimeOut = errors.New("reached converge timeout")

// convergeClusterWithTimeout waits up to the geiven timeout until all given shards reached `status` and then updates the cluster status
func (r *Restore) convergeClusterWithTimeout(shards []pbm.Shard, status pbm.Status, t time.Duration) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	tout := time.NewTicker(t)
	defer tout.Stop()
	for {
		select {
		case <-tk.C:
			ok, err := r.converged(shards, status)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
		case <-tout.C:
			return errConvergeTimeOut
		case <-r.cn.Context().Done():
			return nil
		}
	}
}

func (r *Restore) converged(shards []pbm.Shard, status pbm.Status) (bool, error) {
	shardsToFinish := len(shards)
	bmeta, err := r.cn.GetRestoreMeta(r.name)
	if err != nil {
		return false, errors.Wrap(err, "get backup metadata")
	}

	clusterTime, err := r.cn.ClusterTime()
	if err != nil {
		return false, errors.Wrap(err, "read cluster time")
	}

	for _, sh := range shards {
		for _, shard := range bmeta.Replsets {
			if shard.Name == sh.ID {
				// check if node alive
				lock, err := r.cn.GetLockData(&pbm.LockHeader{
					Type:       pbm.CmdRestore,
					BackupName: r.name,
					Replset:    shard.Name,
				}, pbm.LockCollection)

				// nodes are cleaning its locks moving to the done status
				// so no lock is ok and not need to ckech the heartbeats
				if status != pbm.StatusDone && err != mongo.ErrNoDocuments {
					if err != nil {
						return false, errors.Wrapf(err, "unable to read lock for shard %s", shard.Name)
					}
					if lock.Heartbeat.T+pbm.StaleFrameSec < clusterTime.T {
						return false, errors.Errorf("lost shard %s, last beat ts: %d", shard.Name, lock.Heartbeat.T)
					}
				}

				// check status
				switch shard.Status {
				case status:
					shardsToFinish--
				case pbm.StatusError:
					bmeta.Status = pbm.StatusError
					bmeta.Error = shard.Error
					return false, errors.Errorf("restore on the shard %s failed with: %s", shard.Name, shard.Error)
				}
			}
		}
	}

	if shardsToFinish == 0 {
		err := r.cn.ChangeRestoreState(r.name, status, "")
		if err != nil {
			return false, errors.Wrapf(err, "update backup meta with %s", status)
		}
		return true, nil
	}

	return false, nil
}

func (r *Restore) waitForStatus(status pbm.Status) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	for {
		select {
		case <-tk.C:
			meta, err := r.cn.GetRestoreMeta(r.name)
			if err != nil {
				return errors.Wrap(err, "get restore metadata")
			}

			clusterTime, err := r.cn.ClusterTime()
			if err != nil {
				return errors.Wrap(err, "read cluster time")
			}

			if meta.Hb.T+pbm.StaleFrameSec < clusterTime.T {
				return errors.Errorf("restore stuck, last beat ts: %d", meta.Hb.T)
			}

			switch meta.Status {
			case status:
				return nil
			case pbm.StatusError:
				return errors.Wrap(err, "restore failed")
			}
		case <-r.cn.Context().Done():
			return nil
		}
	}
}

// MarkFailed sets the restore and rs state as failed with the given message
func (r *Restore) MarkFailed(e error) error {
	err := r.cn.ChangeRestoreState(r.name, pbm.StatusError, e.Error())
	if err != nil {
		return errors.Wrap(err, "set backup state")
	}
	err = r.cn.ChangeRestoreRSState(r.name, r.nodeInfo.SetName, pbm.StatusError, e.Error())
	return errors.Wrap(err, "set replset state")
}

func (r *Restore) getMetaFromStore(bcpName string) (*pbm.BackupMeta, error) {
	rd, err := r.stg.SourceReader(bcpName + pbm.MetadataFileSuffix)
	if err != nil {
		return nil, errors.Wrap(err, "get from store")
	}
	defer rd.Close()

	b := &pbm.BackupMeta{}
	err = json.NewDecoder(rd).Decode(b)

	return b, errors.Wrap(err, "decode")
}
