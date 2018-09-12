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
	//
	streamLock *sync.Mutex
	stream     pb.Messages_MessagesChatServer
	statusLock *sync.Mutex
	status     pb.Status
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
		LastSeen:       time.Now(),
		status:         pb.Status{},
		streamLock:     &sync.Mutex{},
		statusLock:     &sync.Mutex{},
	}
	return client
}

func (c *Client) Ping() {
	c.streamSend(&pb.ServerMessage{Type: pb.ServerMessage_PING})
}

func (c *Client) GetBackupSource() (string, error) {
	if err := c.streamSend(&pb.ServerMessage{Type: pb.ServerMessage_GET_BACKUP_SOURCE}); err != nil {
		return "", err
	}
	msg, err := c.stream.Recv()
	if err != nil {
		return "", err
	}
	return msg.GetBackupSourceMsg(), nil
}

func (c *Client) GetStatus() (pb.Status, error) {
	if err := c.streamSend(&pb.ServerMessage{Type: pb.ServerMessage_GET_STATUS}); err != nil {
		return pb.Status{}, err
	}
	msg, err := c.stream.Recv()
	if err != nil {
		return pb.Status{}, err
	}
	log.Debugf("grpc/server/clients.go GetStatus recv message: %v\n", msg)
	statusMsg := msg.GetStatusMsg()
	if statusMsg == nil {
		return pb.Status{}, fmt.Errorf("Received nil GetStatus message from client: %s", c.NodeName)
	}

	c.statusLock.Lock()
	c.status = *statusMsg
	c.statusLock.Unlock()
	return *statusMsg, nil
}

func (c *Client) startBackup(opts *pb.StartBackup) error {
	err := c.streamSend(&pb.ServerMessage{
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
	if err != nil {
		return err
	}
	if msg, err := c.stream.Recv(); err != nil {
		return err
	} else if ack := msg.GetAckMsg(); ack == nil {
		return fmt.Errorf("Invalid client response to start backup message. Want 'ack', got %T", msg)
	}
	c.SetDBBackupRunning(true)
	c.SetOplogTailerRunning(true)
	return nil
}

func (c *Client) SetDBBackupRunning(status bool) {
	c.statusLock.Lock()
	c.status.DBBackUpRunning = status
	c.statusLock.Unlock()
}

func (c *Client) SetOplogTailerRunning(status bool) {
	c.statusLock.Lock()
	c.status.OplogBackupRunning = status
	c.statusLock.Unlock()
}

func (c *Client) IsDBBackupRunning() (status bool) {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()
	return c.status.DBBackUpRunning
}

func (c *Client) IsOplogTailerRunning() (status bool) {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()

	return c.status.OplogBackupRunning
}

func (c *Client) StopOplogTail() error {
	log.Debugf("Stopping oplog tail for client: %s", c.NodeName)
	err := c.streamSend(&pb.ServerMessage{
		Type:    pb.ServerMessage_STOP_OPLOG_TAIL,
		Payload: &pb.ServerMessage_StopOplogTailMsg{StopOplogTailMsg: &pb.StopOplogTail{}},
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
	c.statusLock.Lock()
	defer c.statusLock.Unlock()
	return c.status.DBBackUpRunning
}

func (c *Client) IsOplogBackupRunning() bool {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()
	return c.status.OplogBackupRunning
}

func (c *Client) streamSend(msg *pb.ServerMessage) error {
	c.streamLock.Lock()
	defer c.streamLock.Unlock()
	return c.stream.Send(msg)
}
