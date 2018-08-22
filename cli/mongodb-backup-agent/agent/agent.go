package agent

import (
	"fmt"
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
	grpcClientConn *grpc.ClientConn
	grpcClient     *client.Client
	mdbSession     *mgo.Session
	stopChan       chan bool
	wg             *sync.WaitGroup
}

func NewAgent(conn *grpc.ClientConn, mdbSession *mgo.Session, clientID string) (*Agent, error) {
	clusterID, err := cluster.GetClusterID(mdbSession)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot get MongoDB cluster id")
	}

	nodeType, err := getNodeType(mdbSession)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot get node type")
	}

	messagesClient := pb.NewMessagesClient(conn)
	rpcClient, err := client.NewClient(clientID, clusterID, nodeType, messagesClient)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot create the rpc client")
	}

	agent := &Agent{
		clientID:       clientID,
		grpcClientConn: conn,
		mdbSession:     mdbSession,
		grpcClient:     rpcClient,
		wg:             &sync.WaitGroup{},
	}

	return agent, nil
}

func (a *Agent) Start() {
	a.stopChan = make(chan bool)
	go a.processMessages()
	a.grpcClient.StartStreamIO()
}

func (a *Agent) Stop() {
	close(a.stopChan)
	a.wg.Wait()
	a.grpcClient.StopStreamIO()
}

func (a *Agent) processMessages() {
	a.wg.Add(1)
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

		fmt.Printf("%+v\n", inMsg)

		switch inMsg.Type {
		case pb.ServerMessage_GET_STATUS:
			//default:
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
