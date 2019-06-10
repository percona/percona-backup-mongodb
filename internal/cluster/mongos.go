package cluster

import (
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/percona-backup-mongodb/mdbstructs"
)

const (
	// configMongosColl is the mongodb collection storing mongos state
	configMongosColl = "mongos"

	// pingStaleLimit is staleness-limit of a mongos instance state in the "config" db.
	pingStaleLimit = time.Duration(120) * time.Minute

	maxRetries = 5

	sleepBetweenRetries = 1000 * time.Millisecond
)

// GetMongoRouters returns a slice of Mongos instances with a recent "ping"
// time, sorted by the "ping" time to prefer healthy instances. This will
// only succeed on a cluster config server or mongos instance
// Sometimes, usually after a restore, there is a period of time where querying configMongosColl
// returns nothing, even if there are mongoS. That's why there we are retrying.
func GetMongosRouters(session *mgo.Session) ([]*mdbstructs.Mongos, error) {
	routers := []*mdbstructs.Mongos{}
	var err error
	for i := 0; i < maxRetries; i++ {
		err = session.DB(configDB).C(configMongosColl).Find(bson.M{
			"ping": bson.M{
				"$gte": time.Now().Add(-pingStaleLimit),
			},
		}).Sort("-ping").All(&routers)
		if err != nil {
			return nil, err
		}
		if len(routers) == 0 {
			time.Sleep(sleepBetweenRetries * time.Duration(i+1))
			continue
		}
		break
	}

	return routers, err
}
