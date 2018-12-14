package cluster

import (
	"io/ioutil"

	"github.com/globalsign/mgo/bson"
	"github.com/percona/percona-backup-mongodb/internal/testutils"
)

var (
	testSecondary2Host = testutils.GetMongoDBAddr(testutils.MongoDBShard1ReplsetName, "secondary2")
)

func loadBSONFile(file string, out interface{}) error {
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	return bson.Unmarshal(bytes, out)
}
