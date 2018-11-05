package cluster

import (
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/mdbstructs"
)

// configMongosColl is the mongodb collection storing mongos state
const configMongosColl = "mongos"

// pingStaleLimit is staleness-limit of a mongos instance state in
// the "config" db. mongos sends pings every 30 seconds therefore
// a mongos with a 3-minute-old ping has failed 6 mongos->configsvr
// pings
var pingStaleLimit = time.Duration(3) * time.Minute

// GetMongoRouters returns a slice of Mongos instances with a recent "ping"
// time, sorted by the "ping" time to prefer healthy instances. This will
// only succeed on a cluster config server or mongos instance
func GetMongosRouters(session *mgo.Session) ([]*mdbstructs.Mongos, error) {
	routers := []*mdbstructs.Mongos{}
	err := session.DB(configDB).C(configMongosColl).Find(bson.M{
		"ping": bson.M{
			"$gte": time.Now().Add(-pingStaleLimit),
		},
	}).Sort("-ping").All(&routers)
	return routers, err
}
