package pbm

import (
	"go.mongodb.org/mongo-driver/mongo"
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
	Cmd     Command `bson:"cmd"`
	Backup  Backup  `bson:"backup,omitempty"`
	Restore Restore `bson:"restore,omitempty"`
}

type Backup struct {
	Name        string          `bson:"name"`
	Compression CompressionType `bson:"compression"`
	StoreName   string          `bson:"store,omitempty"`
	FromMaster  bool            `bson:"fromMaster"`
}

type Restore struct {
	MetaObj   string `bson:"meta"`
	StoreName string `bson:"store,omitempty"`
}

type PBM struct {
	Conn    *mongo.Client
	configC *mongo.Collection
	logsC   *mongo.Collection
	opC     *mongo.Collection
	cmdC    *mongo.Collection
}

func New(pbmConn *mongo.Client) *PBM {
	return &PBM{
		Conn:    pbmConn,
		configC: pbmConn.Database(DB).Collection(ConfigCollection),
		logsC:   pbmConn.Database(DB).Collection(LogCollection),
		opC:     pbmConn.Database(CmdStreamDB).Collection(OpCollection),
		cmdC:    pbmConn.Database(CmdStreamDB).Collection(CmdStreamCollection),
	}
}
