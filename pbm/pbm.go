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
	Name         string          `bson:"name" json:"name"`
	DumpName     string          `bson:"dump_name" json:"backup_name" `
	OplogName    string          `bson:"oplog_name" json:"oplog_name"`
	Compression  CompressionType `bson:"compression" json:"compression"`
	RsName       string          `bson:"rs_name" json:"rs_name"`
	Store        Storage         `bson:"store" json:"store"`
	MongoVersion string          `bson:"mongodb_version" json:"mongodb_version,omitempty"`
	StartTS      int64           `bson:"start_ts" json:"start_ts"`
	EndTS        int64           `bson:"end_ts" json:"end_ts"`
	Status       Status          `bson:"status" json:"status"`
	Error        string          `bson:"error,omitempty" json:"error,omitempty"`
}

// Status is backup current status
type Status string

const (
	StatusRunnig Status = "runnig"
	StatusDone          = "done"
	StatusError         = "error"
)

func (p *PBM) Reconnect() error {
	err := p.Conn.Disconnect(p.ctx)
	if err != nil {
		return errors.Wrap(err, "disconnect")
	}

	p.Conn, err = mongo.NewClient(options.Client().ApplyURI(p.curl))
	if err != nil {
		return errors.Wrap(err, "new mongo client")
	}
	err = p.Conn.Connect(p.ctx)
	if err != nil {
		return errors.Wrap(err, "mongo connect")
	}

	return errors.Wrap(p.Conn.Ping(p.ctx, nil), "mongo ping")
}

func (p *PBM) UpdateBackupMeta(m *BackupMeta) error {
	err := p.Conn.Database(DB).Collection(BcpCollection).FindOneAndReplace(
		p.ctx,
		bson.D{{"name", m.Name}, {"rs_name", m.RsName}},
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
