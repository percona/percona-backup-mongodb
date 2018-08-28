package client

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/internal/cluster"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type Client struct {
	clientID   string
	mdbSession *mgo.Session
	grpcClient pb.MessagesClient
	stream     pb.Messages_MessagesChatClient
	cancelFunc context.CancelFunc
}

func NewClient(mdbSession *mgo.Session, conn *grpc.ClientConn) (*Client, error) {
	replset, err := cluster.NewReplset(mdbSession)
	if err != nil {
		return nil, err
	}

	clusterIDString := ""
	if clusterID, _ := cluster.GetClusterID(mdbSession); clusterID != nil {
		clusterIDString = clusterID.Hex()
	}

	nodeType, nodeName, err := getNodeTypeAndName(mdbSession)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot get node type")
	}

	grpcClient := pb.NewMessagesClient(conn)

	ctx, cancel := context.WithCancel(context.TODO())
	stream, err := grpcClient.MessagesChat(ctx)

	m := &pb.ClientMessage{
		Type:     pb.ClientMessage_REGISTER,
		ClientID: fmt.Sprintf("%05d", rand.Int63n(1000000)),
		Payload: &pb.ClientMessage_RegisterMsg{
			RegisterMsg: &pb.RegisterPayload{
				NodeType:       nodeType,
				NodeName:       nodeName,
				ClusterID:      clusterIDString,
				ReplicasetName: replset.Name(),
				ReplicasetID:   replset.ID().Hex(),
			},
		},
	}

	if err := stream.Send(m); err != nil {
		cancel()
		return nil, errors.Wrap(err, "Failed to send registration message")
	}

	response, err := stream.Recv()
	if err != nil {
		cancel()
		return nil, errors.Wrap(err, "Error while receiving the registration response from the server")
	}
	if response.Type != pb.ServerMessage_REGISTRATION_OK {
		cancel()
		return nil, fmt.Errorf("Invalid registration response type: %d", response.Type)
	}

	c := &Client{
		clientID:   nodeName,
		grpcClient: grpcClient,
		mdbSession: mdbSession,
		stream:     stream,
		cancelFunc: cancel,
	}

	// start listening server messages
	go c.processIncommingServerMessages()

	return c, nil
}

func (c *Client) Stop() {
	c.stream.CloseSend()
	c.cancelFunc()
}

func (c *Client) processIncommingServerMessages() {
	for {
		msg, err := c.stream.Recv()
		if err != nil {
			log.Printf("Error reading client incoming messages: %s", err)
			return
		}

		switch msg.Type {
		case pb.ServerMessage_PING:
			c.processPing()
		case pb.ServerMessage_GET_BACKUP_SOURCE:
			c.processGetBackupSource()
		case pb.ServerMessage_GET_STATUS:
		default:
			log.Printf("Unknown message type: %v", msg.Type)
			c.stream.Send(&pb.ClientMessage{
				Type:     pb.ClientMessage_ERROR,
				ClientID: c.clientID,
				Payload:  &pb.ClientMessage_ErrorMsg{ErrorMsg: fmt.Sprintf("Message type %v is not implemented yet", msg.Type)},
			})
		}
	}
}

func (c *Client) processPing() {
	c.stream.Send(&pb.ClientMessage{
		Type:     pb.ClientMessage_PONG,
		ClientID: c.clientID,
		Payload:  &pb.ClientMessage_PingMsg{PingMsg: &pb.PongPayload{Timestamp: time.Now().Unix()}},
	})
}

func (c *Client) processGetBackupSource() {
	r, err := cluster.NewReplset(c.mdbSession)
	if err != nil {
		c.stream.Send(&pb.ClientMessage{
			Type:     pb.ClientMessage_ERROR,
			ClientID: c.clientID,
			Payload:  &pb.ClientMessage_ErrorMsg{ErrorMsg: err.Error()},
		})
		return
	}

	winner, err := r.BackupSource(nil)
	if err != nil {
		c.stream.Send(&pb.ClientMessage{
			Type:     pb.ClientMessage_ERROR,
			ClientID: c.clientID,
			Payload:  &pb.ClientMessage_ErrorMsg{ErrorMsg: fmt.Sprintf("Cannot get backoup source: %s", err)},
		})
		return
	}

	c.stream.Send(&pb.ClientMessage{
		Type:     pb.ClientMessage_BACKUP_SOURCE,
		ClientID: c.clientID,
		Payload:  &pb.ClientMessage_BackupSourceMsg{BackupSourceMsg: winner},
	})
}

func getNodeTypeAndName(session *mgo.Session) (pb.NodeType, string, error) {
	isMaster, err := cluster.NewIsMaster(session)
	if err != nil {
		return pb.NodeType_UNDEFINED, "", err
	}
	if isMaster.IsShardServer() {
		return pb.NodeType_MONGOD_SHARDSVR, isMaster.IsMasterDoc().Me, nil
	}
	if isMaster.IsReplset() {
		return pb.NodeType_MONGOD_REPLSET, isMaster.IsMasterDoc().Me, nil
	}
	if isMaster.IsConfigServer() {
		return pb.NodeType_MONGOD_CONFIGSVR, isMaster.IsMasterDoc().Me, nil
	}
	if isMaster.IsMongos() {
		return pb.NodeType_MONGOS, isMaster.IsMasterDoc().Me, nil
	}
	return pb.NodeType_MONGOD, isMaster.IsMasterDoc().Me, nil
}
