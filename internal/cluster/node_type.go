package cluster

import "github.com/globalsign/mgo"

const (
	NodeTypeUndefined = iota
	NodeTypeMongodShardSvr
	NodeTypeMongodReplicaset
	NodeTypeMongodConfigSvr
	NodeTypeMongos
	NodeTypeMongod
)

func getNodeType(session *mgo.Session) (int, error) {
	isMaster, err := NewIsMaster(session)
	if err != nil {
		return NodeTypeUndefined, err
	}
	if isMaster.IsShardServer() {
		return NodeTypeMongodShardSvr, nil
	}
	if isMaster.IsReplset() {
		return NodeTypeMongodReplicaset, nil
	}
	if isMaster.IsConfigServer() {
		return NodeTypeMongodConfigSvr, nil
	}
	if isMaster.IsMongos() {
		return NodeTypeMongos, nil
	}
	return NodeTypeMongod, nil
}
