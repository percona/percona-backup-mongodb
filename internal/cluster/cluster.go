package cluster

import (
	"strings"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/mdbstructs"
)

type Shard struct {
	id      string
	uri     string
	replset *Replset
}

func parseShardURI(uri string) (string, []string) {
	s := strings.Split(uri, "/")
	if len(s) == 2 {
		return s[0], strings.Split(s[1], ",")
	}
	return "", []string{}
}

func NewShard(shard *mdbstructs.ListShardsShard) *Shard {
	replset, addrs := parseShardURI(shard.Host)
	return &Shard{
		id:  shard.Id,
		uri: uri,
		replset: &Replset{
			name:  replset,
			addrs: addrs,
		},
	}
}

// Return the shards within a sharded cluster using the MongoDB 'listShards'
// server command. This command will only succeed on a mongos or config
// server.
//
// https://docs.mongodb.com/manual/reference/command/listShards/
//
func GetListShards(session *mgo.Session) (*mdbstructs.ListShards, error) {
	listShards := mdbstructs.ListShards{}
	err := session.Run(bson.D{{"listShards", "1"}}, &listShards)
	return &listShards, err
}
