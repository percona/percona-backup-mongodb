package backup

import (
	"context"
	"errors"
	"log"
	"os"
	"testing"

	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/storage/fs"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
	"github.com/percona/percona-backup-mongodb/pbm/version"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type TestEnvironment struct {
	Mongo      *mongodb.MongoDBContainer
	Client     connect.Client
	Brief      topo.NodeBrief
	PbmStorage Storage
}

func (tenv *TestEnvironment) StartMongo(ctx context.Context) error {
	cnt, err := mongodb.Run(
		ctx,
		"perconalab/percona-server-mongodb:8.0.4-multi",
		mongodb.WithReplicaSet("rs0"),
	)
	if err != nil {
		return err
	}

	cs, err := cnt.ConnectionString(ctx)
	if err != nil {
		return err
	}
	cs += "&directConnection=true"

	client, err := connect.Connect(ctx, cs, "test")
	if err != nil {
		return err
	}

	tenv.Client = client
	tenv.Mongo = cnt

	info, err := topo.GetNodeInfo(ctx, client.MongoClient())
	if err != nil {
		return err
	}
	mongoVersion, err := version.GetMongoVersion(ctx, client.MongoClient())
	if err != nil {
		return err
	}

	tenv.Brief = topo.NodeBrief{
		URI:       cs,
		SetName:   info.SetName,
		Me:        info.Me,
		Sharded:   info.IsSharded(),
		ConfigSvr: info.IsConfigSrv(),
		Version:   mongoVersion,
	}

	return nil
}

func (tenv *TestEnvironment) StopMongo(ctx context.Context) error {
	return tenv.Mongo.Terminate(ctx)
}

func (tenv *TestEnvironment) Cleanup(ctx context.Context) error {
	mongoErr := tenv.StopMongo(ctx)
	stgErr := tenv.cleanupStorage()

	return errors.Join(mongoErr, stgErr)
}

func (tenv *TestEnvironment) Reset(t *testing.T) {
	tenv.ResetWithConfig(t, &config.Config{})
}

func (tenv *TestEnvironment) ResetWithConfig(t *testing.T, cfg *config.Config) {
	if err := tenv.cleanupStorage(); err != nil {
		assert.FailNow(t, "failed to reset storage: %v", err)
	}
	if err := tenv.resetMongo(t.Context()); err != nil {
		assert.FailNow(t, "failed to reset mongo: %v", err)
	}
	path, err := tempDir(cfg.Name)
	if err != nil {
		assert.FailNow(t, "failed to create storage dir: %v", err)
	}

	cfg.Name = ""
	cfg.IsProfile = false
	cfg.Storage = config.StorageConf{
		Type: storage.Filesystem,
		Filesystem: &fs.Config{
			Path: path,
		},
	}

	if err = tenv.SetConfig(cfg); err != nil {
		assert.FailNow(t, "failed to set config: %v", err)
	}

	tenv.PbmStorage = Storage{
		IsProfile:   false,
		Name:        "",
		StorageConf: cfg.Storage,
	}
}

func (tenv *TestEnvironment) cleanupStorage() error {
	return os.RemoveAll(tenv.PbmStorage.Path())
}

func (tenv *TestEnvironment) resetMongo(ctx context.Context) error {
	// clear admin.pbm* collections
	mongo := tenv.Client.MongoClient()

	// list and drop databases except local, config and admin
	sysDBs := bson.A{"local", "config", "admin", defs.DB}
	sysFilter := bson.D{{Key: "name", Value: bson.D{{Key: "$nin", Value: sysDBs}}}}
	dbs, err := mongo.ListDatabaseNames(ctx, sysFilter)
	if err != nil {
		return err
	}

	for _, db := range dbs {
		err = mongo.Database(db).Drop(ctx)
		if err != nil {
			return err
		}
	}

	// drop PBM collections
	db := mongo.Database(defs.DB)
	pbmColFilter := bson.D{{Key: "name", Value: primitive.Regex{Pattern: "^pbm"}}}
	collections, err := db.ListCollectionNames(ctx, pbmColFilter)
	if err != nil {
		return err
	}
	for _, coll := range collections {
		err = db.Collection(coll).Drop(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (tenv *TestEnvironment) SetConfig(cfg *config.Config) error {
	if cfg.IsProfile {
		return config.AddProfile(context.Background(), tenv.Client, cfg)
	}
	return config.SetConfig(context.Background(), tenv.Client, cfg)
}

func tempDir(name string) (string, error) {
	return os.MkdirTemp("", "pbm_"+name+"_*")
}

// TempStorageProfile creates a temporary FS storage profile,
// the path gets removed on test cleanup.
func TempStorageProfile(t *testing.T, name string) Storage {
	path, err := tempDir(name)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(path)
	})

	stgConf := config.StorageConf{
		Type: storage.Filesystem,
		Filesystem: &fs.Config{
			Path: path,
		},
	}

	return Storage{
		IsProfile:   name != "",
		Name:        name,
		StorageConf: stgConf,
	}
}

var TestEnv *TestEnvironment

func TestMain(m *testing.M) {
	TestEnv = &TestEnvironment{}
	ctx := context.Background()

	err := TestEnv.StartMongo(ctx)
	if err != nil {
		log.Fatalf("failed to start test MongoDB: %s", err)
	}

	defer func() {
		err = TestEnv.Cleanup(ctx)
		if err != nil {
			log.Fatalf("failed to cleanup test environment: %s", err)
		}
	}()

	code := m.Run()
	os.Exit(code)
}
