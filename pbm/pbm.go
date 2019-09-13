package pbm

import (
	"context"
	"net/url"
	"strings"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

const (
	// DB is a name of the PBM database
	DB = "admin"
	// LogCollection is the name of the mongo collection that contains PBM logs
	LogCollection = "pbmLog"
	// ConfigCollection is the name of the mongo collection that contains PBM configs
	ConfigCollection = "pbmConfig"
	// OpCollection is the name of the mongo collection that is used
	// by agents to coordinate operations (e.g. locks)
	OpCollection = "pbmOp"
	// BcpCollection is a collection for backups metadata
	BcpCollection = "pbmBackups"

	// CmdStreamCollection is the name of the mongo collection that contains backup/restore commands stream
	CmdStreamCollection = "pbmCmd"

	// NoReplset is the name of a virtual replica set of the standalone node
	NoReplset = "pbmnoreplicaset"
)

type Command string

const (
	CmdUndefined Command = ""
	CmdBackup            = "backup"
	CmdRestore           = "restore"
)

type Cmd struct {
	Cmd     Command    `bson:"cmd"`
	Backup  BackupCmd  `bson:"backup,omitempty"`
	Restore RestoreCmd `bson:"restore,omitempty"`
	TS      int64      `bson:"ts"`
}

type BackupCmd struct {
	Name        string          `bson:"name"`
	Compression CompressionType `bson:"compression"`
	StoreName   string          `bson:"store,omitempty"`
	FromMaster  bool            `bson:"fromMaster"`
}

type RestoreCmd struct {
	BackupName string `bson:"backupName"`
	StoreName  string `bson:"store,omitempty"`
}

type CompressionType string

const (
	CompressionTypeNone   CompressionType = "none"
	CompressionTypeGZIP                   = "gzip"
	CompressionTypeSNAPPY                 = "snappy"
	CompressionTypeLZ4                    = "lz4"
)

type PBM struct {
	Conn *mongo.Client
	ctx  context.Context
}

func New(ctx context.Context, uri string) (*PBM, error) {
	uri = "mongodb://" + strings.Replace(uri, "mongodb://", "", 1)

	client, err := connect(ctx, uri)
	if err != nil {
		return nil, errors.Wrap(err, "create mongo connection")
	}
	pbm := &PBM{
		Conn: client,
		ctx:  ctx,
	}

	im, err := pbm.GetIsMaster()
	if err != nil {
		return nil, errors.Wrap(err, "get topology")
	}

	if !im.IsSharded() || im.ReplsetRole() == ReplRoleConfigSrv {
		return pbm, nil
	}

	csvr := struct {
		URI string `bson:"configsvrConnectionString"`
	}{}
	err = client.Database("admin").Collection("system.version").
		FindOne(ctx, bson.D{{"_id", "shardIdentity"}}).Decode(&csvr)
	// no need in this connection anymore
	client.Disconnect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get config server connetion URI")
	}

	chost := strings.Split(csvr.URI, "/")
	if len(chost) < 2 {
		return nil, errors.Wrapf(err, "define config server connetion URI from %s", csvr.URI)
	}

	curi, err := url.Parse(uri)
	if err != nil {
		return nil, errors.Wrap(err, "parse mongo-uri")
	}

	q := curi.Query()
	q.Set("replicaSet", chost[0])
	curi.RawQuery = q.Encode()
	curi.Host = chost[1]
	pbm.Conn, err = connect(ctx, curi.String())
	if err != nil {
		return nil, errors.Wrap(err, "create mongo connection to configsvr")
	}

	return pbm, errors.Wrap(pbm.setupNewDB(), "setup a new backups db")
}

// setup a new DB for PBM
func (p *PBM) setupNewDB() error {
	err := p.Conn.Database(DB).RunCommand(
		p.ctx,
		bson.D{{"create", CmdStreamCollection}, {"capped", true}, {"size", 1 << 10 * 2}},
	).Err()
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return errors.Wrap(err, "ensure cmd collection")
	}

	err = p.Conn.Database(DB).RunCommand(
		p.ctx,
		bson.D{{"create", OpCollection}}, //size 2kb ~ 10 commands
	).Err()
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return errors.Wrap(err, "ensure lock collection")
	}

	return nil
}

func connect(ctx context.Context, uri string) (*mongo.Client, error) {
	client, err := mongo.NewClient(
		options.Client().ApplyURI(uri).
			SetReadPreference(readpref.Primary()).
			SetReadConcern(readconcern.Majority()).
			SetWriteConcern(writeconcern.New(writeconcern.WMajority())),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create mongo client")
	}
	err = client.Connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "mongo connect")
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "mongo ping")
	}

	return client, nil
}

// BackupMeta is a backup's metadata
// ! any changes should be reflected in *PBM.UpdateBackupMeta()
type BackupMeta struct {
	Name         string          `bson:"name" json:"name"`
	Replsets     []BackupReplset `bson:"replsets" json:"replsets"`
	Compression  CompressionType `bson:"compression" json:"compression"`
	Store        Storage         `bson:"store" json:"store"`
	MongoVersion string          `bson:"mongodb_version" json:"mongodb_version,omitempty"`
	StartTS      int64           `bson:"start_ts" json:"start_ts"`
	DoneTS       int64           `bson:"done_ts" json:"done_ts"`
	Status       Status          `bson:"status" json:"status"`
	Error        string          `bson:"error,omitempty" json:"error,omitempty"`
}

type BackupReplset struct {
	Name        string `bson:"name" json:"name"`
	DumpName    string `bson:"dump_name" json:"backup_name" `
	OplogName   string `bson:"oplog_name" json:"oplog_name"`
	Status      Status `bson:"status" json:"status"`
	Error       string `bson:"error,omitempty" json:"error,omitempty"`
	StartTS     int64  `bson:"start_ts" json:"start_ts"`
	DumpDoneTS  int64  `bson:"dump_done_ts" json:"dump_done_ts"`
	OplogDoneTS int64  `bson:"oplog_done_ts" json:"oplog_done_ts"`
}

// Status is backup current status
type Status string

const (
	StatusRunnig   Status = "runnig"
	StatusDumpDone        = "dumpDone"
	StatusDone            = "done"
	StatusError           = "error"
)

func (p *PBM) UpdateBackupMeta(m *BackupMeta) error {
	// TODO: BackupMeta fileds changes depends on this code
	set := bson.M{
		"name":            m.Name,
		"compression":     m.Compression,
		"store":           m.Store,
		"mongodb_version": m.MongoVersion,
		"start_ts":        m.StartTS,
		"done_ts":         m.DoneTS,
		"status":          m.Status,
		"error":           m.Error,
	}

	_, err := p.Conn.Database(DB).Collection(BcpCollection).UpdateOne(
		p.ctx,
		bson.D{{"name", m.Name}},
		bson.M{"$set": set},
		options.Update().SetUpsert(true),
	)

	return err
}

func (p *PBM) AddShardToBackupMeta(bcpName string, shard BackupReplset) error {
	_, err := p.Conn.Database(DB).Collection(BcpCollection).UpdateOne(
		p.ctx,
		bson.D{{"name", bcpName}},
		bson.D{{"$addToSet", bson.M{"replsets": shard}}},
	)

	return err
}

func (p *PBM) GetBackupMeta(name string) (*BackupMeta, error) {
	b := new(BackupMeta)
	res := p.Conn.Database(DB).Collection(BcpCollection).FindOne(p.ctx, bson.D{{"name", name}})
	if res.Err() != nil {
		return nil, errors.Wrap(res.Err(), "get")
	}
	err := res.Decode(b)
	return b, errors.Wrap(err, "decode")
}

func (p *PBM) BackupsList(limit int64) ([]BackupMeta, error) {
	cur, err := p.Conn.Database(DB).Collection(BcpCollection).Find(
		p.ctx,
		bson.M{},
		options.Find().SetLimit(limit).SetSort(bson.D{{"start_ts", 1}}),
	)
	if err != nil {
		return nil, errors.Wrap(err, "query mongo")
	}

	defer cur.Close(p.ctx)

	backups := []BackupMeta{}
	for cur.Next(p.ctx) {
		b := BackupMeta{}
		err := cur.Decode(&b)
		if err != nil {
			return nil, errors.Wrap(err, "message decode")
		}
		backups = append(backups, b)
	}

	return backups, cur.Err()
}

// GetShards gets list of shards
func (p *PBM) GetShards() ([]Shard, error) {
	cur, err := p.Conn.Database("config").Collection("shards").Find(p.ctx, bson.M{})
	if err != nil {
		return nil, errors.Wrap(err, "query mongo")
	}

	defer cur.Close(p.ctx)

	shards := []Shard{}
	for cur.Next(p.ctx) {
		s := Shard{}
		err := cur.Decode(&s)
		if err != nil {
			return nil, errors.Wrap(err, "message decode")
		}
		shards = append(shards, s)
	}

	return shards, cur.Err()
}

// Context returns object context
func (p *PBM) Context() context.Context {
	return p.ctx
}

func (p *PBM) GetIsMaster() (*IsMaster, error) {
	im := &IsMaster{}
	err := p.Conn.Database(DB).RunCommand(p.ctx, bson.D{{"isMaster", 1}}).Decode(im)
	if err != nil {
		return nil, errors.Wrap(err, "run mongo command isMaster")
	}
	return im, nil
}
