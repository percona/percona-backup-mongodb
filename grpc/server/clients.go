package server

import (
	"fmt"
	"sync"
	"time"

	pb "github.com/percona/mongodb-backup/proto/messages"
)

var (
	ClientAlreadyExistsError = fmt.Errorf("Client ID already registered")
	UnknownClientID          = fmt.Errorf("Unknown client ID")
)

type Client struct {
	ID              string      `json:"id"`
	NodeType        pb.NodeType `json:"node_type"`
	NodeName        string      `json:"node_name"`
	ClusterID       string      `json:"client_id"`
	ReplicasetName  string      `json:"replicaset_name"`
	ReplicasetID    string      `json:"replicasert_id"`
	LastCommandSent string      `json:"last_command_ent"`
	LastSeen        time.Time   `json:"last_seen"`
	Status          pb.Status   `json:"Status"`
	//
	stream pb.Messages_MessagesChatServer
	lock   *sync.Mutex
}

func NewClient(id, clusterID, nodeName, replicasetID, replicasetName string, nodeType pb.NodeType, stream pb.Messages_MessagesChatServer) *Client {
	client := &Client{
		ID:             id,
		ClusterID:      clusterID,
		ReplicasetID:   replicasetID,
		ReplicasetName: replicasetName,
		NodeType:       nodeType,
		NodeName:       nodeName,
		stream:         stream,
		lock:           &sync.Mutex{},
		LastSeen:       time.Now(),
	}
	return client
}

func (c *Client) Ping() {
	c.stream.Send(&pb.ServerMessage{Type: pb.ServerMessage_PING})
}

func (c *Client) GetBackupSource() (string, error) {
	c.stream.Send(&pb.ServerMessage{Type: pb.ServerMessage_GET_BACKUP_SOURCE})
	msg, err := c.stream.Recv()
	if err != nil {
		return "", err
	}
	return msg.GetBackupSourceMsg(), nil
}

func (c *Client) GetStatus() (*pb.Status, error) {
	c.stream.Send(&pb.ServerMessage{Type: pb.ServerMessage_GET_STATUS})
	msg, err := c.stream.Recv()
	if err != nil {
		return nil, err
	}
	statusMsg := msg.GetStatusMsg()
	return statusMsg, nil
}
