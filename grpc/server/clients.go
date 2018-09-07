package server

import (
	"fmt"
	"sync"
	"time"

	pb "github.com/percona/mongodb-backup/proto/messages"
	log "github.com/sirupsen/logrus"
)

var (
	ClientAlreadyExistsError = fmt.Errorf("Client ID already registered")
	UnknownClientID          = fmt.Errorf("Unknown client ID")
	timeout                  = 1 * time.Second
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
	Status          *pb.Status  `json:"Status"`
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
		Status:         &pb.Status{},
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
	c.lock.Lock()
	defer c.lock.Unlock()
	c.stream.Send(&pb.ServerMessage{Type: pb.ServerMessage_GET_STATUS})
	msg, err := c.stream.Recv()
	if err != nil {
		return nil, err
	}
	statusMsg := msg.GetStatusMsg()
	c.Status = statusMsg
	return statusMsg, nil
}

func (c *Client) StartBackup(opts *pb.StartBackup) error {
	c.setDBBackupRunning(true)
	c.stream.Send(&pb.ServerMessage{
		Type: pb.ServerMessage_START_BACKUP,
		Payload: &pb.ServerMessage_StartBackupMsg{
			StartBackupMsg: &pb.StartBackup{
				BackupType:      opts.BackupType,
				DestinationType: opts.DestinationType,
				DestinationName: opts.DestinationName,
				DestinationDir:  opts.DestinationDir,
				CompressionType: opts.CompressionType,
				Cypher:          opts.Cypher,
				OplogStartTime:  opts.OplogStartTime,
			},
		},
	})
	if msg, err := c.stream.Recv(); err != nil {
		c.setDBBackupRunning(false)
		return err
	} else if ack := msg.GetAckMsg(); ack == nil {
		c.setDBBackupRunning(false)
		return fmt.Errorf("Invalid client response to start backup message. Want 'ack', got %T", msg)
	}
	return nil
}

func (c *Client) setDBBackupRunning(status bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.Status.DBBackUpRunning = status
}

func (c *Client) StopOplogTail() error {
	log.Printf("Stopping oplog tail")
	err := c.stream.Send(&pb.ServerMessage{
		Type:    pb.ServerMessage_STOP_OPLOG_TAIL,
		Payload: &pb.ServerMessage_StopOplogTailMsg{&pb.StopOplogTail{}},
	})
	if err != nil {
		log.Printf("Error in client.StopOplogTail stream.Send(...): %s", err)
	}

	if msg, err := c.stream.Recv(); err != nil {
		return err
	} else if ack := msg.GetAckMsg(); ack == nil {
		return fmt.Errorf("Invalid client response to start backup message. Want 'ack', got %T", msg)
	}
	return nil
}

func (c *Client) IsDbBackupRunning() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.Status.DBBackUpRunning
}

func (c *Client) IsOplogBackupRunning() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.Status.OplogBackupRunning
}
