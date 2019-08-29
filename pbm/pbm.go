package pbm

import (
	"context"

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
	Conn    *mongo.Client
	configC *mongo.Collection
	logsC   *mongo.Collection
	opC     *mongo.Collection
	cmdC    *mongo.Collection
	bcpC    *mongo.Collection
}

func New(pbmConn *mongo.Client) *PBM {
	return &PBM{
		Conn:    pbmConn,
		configC: pbmConn.Database(DB).Collection(ConfigCollection),
		logsC:   pbmConn.Database(DB).Collection(LogCollection),
		bcpC:    pbmConn.Database(DB).Collection(BcpCollection),
		opC:     pbmConn.Database(CmdStreamDB).Collection(OpCollection),
		cmdC:    pbmConn.Database(CmdStreamDB).Collection(CmdStreamCollection),
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

func (p *PBM) UpdateBackupMeta(m *BackupMeta) error {
	err := p.bcpC.FindOneAndReplace(
		context.Background(),
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
	err := p.bcpC.FindOne(context.Background(), bson.D{{"name", name}}).Decode(b)
	return b, err
}
