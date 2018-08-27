package agent

import (
	"log"
	"sync"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/grpc/client"
	"github.com/percona/mongodb-backup/internal/cluster"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type Agent struct {
	clientID       string
	clusterID      *bson.ObjectId
	nodeType       pb.NodeType
	replicasetName string
	replicasetID   string
	status         *pb.Status
	//
	grpcClientConn *grpc.ClientConn
	grpcClient     *client.Client
	mdbSession     *mgo.Session
	stopChan       chan bool
	wg             *sync.WaitGroup
}

func NewAgent(conn *grpc.ClientConn, mdbSession *mgo.Session, clientID string) (*Agent, error) {
	clusterID, err := cluster.GetClusterID(mdbSession)
	if err != nil {
		log.Printf("Cannot get cluster id: %s", err)
	}

	nodeType, err := getNodeType(mdbSession)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot get node type")
	}

	replset, err := cluster.NewReplset(mdbSession)
	if err != nil {
		return nil, err
	}

	messagesClient := pb.NewMessagesClient(conn)
	rpcClient, err := client.NewClient(clientID, replset.Name(), clusterID, replset.ID(), nodeType, messagesClient)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot create the rpc client")
	}

	agent := &Agent{
		clientID:       clientID,
		nodeType:       nodeType,
		grpcClientConn: conn,
		mdbSession:     mdbSession,
		grpcClient:     rpcClient,
		replicasetName: replset.Name(),
		replicasetID:   replset.ID().Hex(),
		wg:             &sync.WaitGroup{},
	}

	return agent, nil
}

func (a *Agent) Start() {
	a.stopChan = make(chan bool)
	a.wg.Add(1)
	go a.processMessages()
	a.grpcClient.StartStreamIO()
}

func (a *Agent) Stop() {
	close(a.stopChan)
	a.wg.Wait()
	a.grpcClient.StopStreamIO()
}

func (a *Agent) processMessages() {
	defer a.wg.Done()
	for {
		var inMsg *pb.ServerMessage
		select {
		case inMsg = <-a.grpcClient.InMsgChan():
			if inMsg == nil {
				return
			}
		case <-a.stopChan:
			return
		}

		switch inMsg.Type {
		case pb.ServerMessage_GET_STATUS:
			outmsg := &pb.ClientMessage{
				ClientID: a.clientID,
				Type:     pb.ClientMessage_STATUS,
				Payload: &pb.ClientMessage_StatusMsg{
					StatusMsg: a.status,
				},
			}
			a.grpcClient.OutMsgChan() <- outmsg
		}
	}
}

func getNodeType(session *mgo.Session) (pb.NodeType, error) {
	isMaster, err := cluster.NewIsMaster(session)
	if err != nil {
		return pb.NodeType_UNDEFINED, err
	}
	if isMaster.IsShardServer() {
		return pb.NodeType_MONGOD_SHARDSVR, nil
	}
	if isMaster.IsReplset() {
		return pb.NodeType_MONGOD_REPLSET, nil
	}
	if isMaster.IsConfigServer() {
		return pb.NodeType_MONGOD_CONFIGSVR, nil
	}
	if isMaster.IsMongos() {
		return pb.NodeType_MONGOS, nil
	}
	return pb.NodeType_MONGOD, nil
}
