package cluster

import (
	"fmt"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

// configMongosColl is the mongodb collection storing mongos state
const configMongosColl = "mongos"

// pingStaleLimit is staleness-limit of a mongos instance's state in
// the "config" db. mongos sends pings every 30 seconds so the "ping"
// field should be recent on healthy mongos instances
var pingStaleLimit = time.Duration(-3) * time.Minute

// Mongos reflects the cluster state of a mongos instance using the
// "config.mongos" collection stored on config server
//
// See: https://docs.mongodb.com/manual/reference/config-database/#config.mongos
//
type Mongos struct {
	Id                string    `bson:"_id"`
	Ping              time.Time `bson:"ping"`
	Up                int64     `bson:"up"`
	MongoVersion      string    `bson:"mongoVersion"`
	AdvisoryHostFQDNs []string  `bson:"advisoryHostFQDNs,omitempty"`
	Waiting           bool      `bson:"waiting,omitempty"`
}

// Addr returns the address of the mongos as a string
func (m *Mongos) Addr() string {
	return m.Id
}

// GetMongoRouters returns a slice of Mongos instances with a recent "ping"
// time, sorted by the "ping" time to prefer healthy instances. This will
// only succeed on a cluster config server or mongos instance
func GetMongosRouters(session *mgo.Session) ([]*Mongos, error) {
	routers := []*Mongos{}
	err := session.DB(configDB).C(configMongosColl).Find(bson.M{
		"ping": bson.M{
			"$gte": time.Now().Add(pingStaleLimit),
		},
	}).Sort("-ping").All(&routers)
	return routers, err
}
