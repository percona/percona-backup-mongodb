package mdbstructs

import (
	"time"

	"github.com/globalsign/mgo/bson"
)

// IsMaster represents the document returned by db.runCommand( { isMaster: 1 } )
type IsMaster struct {
	Hosts                        []string  `bson:"hosts"`
	IsMaster                     bool      `bson:"ismaster"`
	Msg                          string    `bson:"msg"`
	MaxBsonObjectSise            int64     `bson:"maxBsonObjectSize"`
	MaxMessageSizeBytes          int64     `bson:"maxMessageSizeBytes"`
	MaxWriteBatchSize            int64     `bson:"maxWriteBatchSize"`
	LocalTime                    time.Time `bson:"localTime"`
	LogicalSessionTimeoutMinutes int64     `bson:"logicalSessionTimeoutMinutes"`
	MaxWireVersion               int64     `bson:"maxWireVersion"`
	MinWireVersion               int64     `bson:"minWireVersion"`
	Ok                           int       `bson:"ok"`
	SetName                      string    `bson:"setName,omitempty"`
	SetVersion                   string    `bson:"setVersion"`
	Primary                      string    `bson:"primary"`
	Secondary                    bool      `bson:"secondary"`
	ConfigSvr                    int       `bson:"configsvr,omitempty"`
	Me                           string    `bson:"me"`
	LastWrite                    struct {
		OpTime           *OpTime   `bson:"opTime"`
		LastWriteDate    time.Time `bson:"lastWriteDate"`
		MajorityOpTime   *OpTime   `bson:"majorityTime"`
		MajoriyWriteDate time.Time `bson:"majorityWriteDate"`
	} `bson:"lastWrite"`
	ClusterTime       *ClusterTime         `bson:"$clusterTime,omitempty"`
	ConfigServerState *ConfigServerState   `bson:"$configServerState,omitempty"`
	GleStats          *GleStats            `bson:"$gleStats,omitempty"`
	OperationTime     *bson.MongoTimestamp `bson:"operationTime,omitempty"`
}
