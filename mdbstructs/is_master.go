package mdbstructs

import (
	"time"

	"github.com/globalsign/mgo/bson"
)

// IsMaster represents the document returned by db.runCommand( { isMaster: 1 } )
type IsMaster struct {
	Hosts                        []string            `bson:"hosts"`
	IsMaster                     bool                `bson:"ismaster"`
	Msg                          string              `bson:"msg"`
	MaxBsonObjectSise            int64               `bson:"maxBsonObjectSize"`
	MaxMessageSizeBytes          int64               `bson:"maxMessageSizeBytes"`
	MaxWriteBatchSize            int64               `bson:"maxWriteBatchSize"`
	LocalTime                    time.Time           `bson:"localTime"`
	LogicalSessionTimeoutMinutes int64               `bson:"logicalSessionTimeoutMinutes"`
	MaxWireVersion               int64               `bson:"maxWireVersion"`
	MinWireVersion               int64               `bson:"minWireVersion"`
	Ok                           int                 `bson:"ok"`
	ClusterTime                  ClusterTime         `bson:"$clusterTime"`
	OperationTime                bson.MongoTimestamp `bson:"operationTime"`
	SetName                      string              `bson:"setName"`
	SetVersion                   string              `bson:"setVersion"`
	Primary                      string              `bson:"primary"`
	Secondary                    bool                `bson:"secondary"`
	ConfigSvr                    int                 `bson:"configsvr"`
	Me                           string              `bson:"me"`
	LastWrite                    struct {
		OpTime struct {
			Ts bson.MongoTimestamp `bson:"ts"`
			T  int64               `bson:"t"`
		} `bson:"opTime"`
		LastWriteDate  time.Time `bdon:"lastWriteDate"`
		MajorityOpTime struct {
			Ts bson.MongoTimestamp `bson:"ts"`
			T  int64               `bson:"t"`
		} `bson:"majorityTime"`
	} `bson:"lastWrite"`
	GleStats struct {
		LastOpTime bson.MongoTimestamp `bson:"lastOpTime"`
		ElectionID bson.ObjectId       `bson:"electionId"`
	} `bson:"$gleStats"`
	ConfigServerState struct {
		OpTime struct {
			Ts bson.MongoTimestamp `bson:"ts"`
			T  int64               `bson:"t"`
		} `bson:"opTime"`
	} `bson:"$configServerState"`
}
