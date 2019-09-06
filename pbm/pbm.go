package pbm

import (
	"context"

	"github.com/pkg/errors"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
	BcpCollection = "backups"

	// CmdStreamDB is the cmd database
	CmdStreamDB = "pbm"
	// CmdStreamCollection is the name of the mongo collection that contains backup/restore commands stream
	CmdStreamCollection = "cmd"

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
	curl string
}

func New(ctx context.Context, pbmConn *mongo.Client, curl string) *PBM {
	return &PBM{
		Conn: pbmConn,
		curl: curl,
		ctx:  ctx,
	}
}

type BackupMeta struct {
	Name         string                   `bson:"name" json:"name"`
	Replsets     map[string]BackupReplset `bson:"replsets" json:"replsets"`
	Compression  CompressionType          `bson:"compression" json:"compression"`
	Store        Storage                  `bson:"store" json:"store"`
	MongoVersion string                   `bson:"mongodb_version" json:"mongodb_version,omitempty"`
	StartTS      int64                    `bson:"start_ts" json:"start_ts"`
	DoneTS       int64                    `bson:"done_ts" json:"done_ts"`
	Status       Status                   `bson:"status" json:"status"`
	Error        string                   `bson:"error,omitempty" json:"error,omitempty"`
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
	err := p.Conn.Database(DB).Collection(BcpCollection).FindOneAndReplace(
		p.ctx,
		bson.D{{"name", m.Name}},
		m,
		options.FindOneAndReplace().SetUpsert(true),
	).Err()

	if err == mongo.ErrNoDocuments {
		return nil
	}

	return err
}

func (p *PBM) GetBackupMeta(name string) (*BackupMeta, error) {
	b := new(BackupMeta)
	res := p.Conn.Database(DB).Collection(BcpCollection).FindOne(p.ctx, bson.D{{"name", name}})
	if res.Err() != nil {
		if res.Err() == mongo.ErrNoDocuments {
			return nil, errors.New("no backup '" + name + "' found")
		}
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
