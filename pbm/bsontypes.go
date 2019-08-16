package pbm

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type OpTime struct {
	TS   primitive.Timestamp `bson:"ts" json:"ts"`
	Term int64               `bson:"t" json:"t"`
}

// IsMasterLastWrite represents the last write to the MongoDB server
type IsMasterLastWrite struct {
	OpTime            *OpTime   `bson:"opTime"`
	LastWriteDate     time.Time `bson:"lastWriteDate"`
	MajorityOpTime    *OpTime   `bson:"majorityTime"`
	MajorityWriteDate time.Time `bson:"majorityWriteDate"`
}

// IsMaster represents the document returned by db.runCommand( { isMaster: 1 } )
type IsMaster struct {
	Hosts                        []string             `bson:"hosts,omitempty"`
	IsMaster                     bool                 `bson:"ismaster"`
	Msg                          string               `bson:"msg"`
	MaxBsonObjectSise            int64                `bson:"maxBsonObjectSize"`
	MaxMessageSizeBytes          int64                `bson:"maxMessageSizeBytes"`
	MaxWriteBatchSize            int64                `bson:"maxWriteBatchSize"`
	LocalTime                    time.Time            `bson:"localTime"`
	LogicalSessionTimeoutMinutes int64                `bson:"logicalSessionTimeoutMinutes"`
	MaxWireVersion               int64                `bson:"maxWireVersion"`
	MinWireVersion               int64                `bson:"minWireVersion"`
	OK                           int                  `bson:"ok"`
	SetName                      string               `bson:"setName,omitempty"`
	SetVersion                   int32                `bson:"setVersion,omitempty"`
	Primary                      string               `bson:"primary,omitempty"`
	Secondary                    bool                 `bson:"secondary,omitempty"`
	Hidden                       bool                 `bson:"hidden,omitempty"`
	ConfigSvr                    int                  `bson:"configsvr,omitempty"`
	Me                           string               `bson:"me"`
	LastWrite                    IsMasterLastWrite    `bson:"lastWrite"`
	ClusterTime                  *ClusterTime         `bson:"$clusterTime,omitempty"`
	ConfigServerState            *ConfigServerState   `bson:"$configServerState,omitempty"`
	GleStats                     *GleStats            `bson:"$gleStats,omitempty"`
	OperationTime                *primitive.Timestamp `bson:"operationTime,omitempty"`
}

type ClusterTime struct {
	ClusterTime primitive.Timestamp `bson:"clusterTime"`
	Signature   struct {
		Hash  primitive.Binary `bson:"hash"`
		KeyID int64            `bson:"keyId"`
	} `bson:"signature"`
}

type ConfigServerState struct {
	OpTime *OpTime `bson:"opTime"`
}

type GleStats struct {
	LastOpTime primitive.Timestamp `bson:"lastOpTime"`
	ElectionId primitive.ObjectID  `bson:"electionId"`
}

type Operation string

const (
	OperationInsert  Operation = "i"
	OperationNoop    Operation = "n"
	OperationUpdate  Operation = "u"
	OperationDelete  Operation = "d"
	OperationCommand Operation = "c"
)
