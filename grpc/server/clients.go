package server

import (
	"fmt"
	"sync"
	"time"

	pb "github.com/percona/mongodb-backup/proto/messages"
	"github.com/sirupsen/logrus"
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
	ClusterID       string      `json:"cluster_id"`
	ReplicasetName  string      `json:"replicaset_name"`
	ReplicasetUUID  string      `json:"replicaset_uuid"`
	LastCommandSent string      `json:"last_command_sent"`
	LastSeen        time.Time   `json:"last_seen"`
	logger          *logrus.Logger
	//
	streamLock *sync.Mutex
	stream     pb.Messages_MessagesChatServer
	statusLock *sync.Mutex
	status     pb.Status
}

func newClient(id, clusterID, nodeName, replicasetUUID, replicasetName string, nodeType pb.NodeType,
	stream pb.Messages_MessagesChatServer, logger *logrus.Logger) *Client {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.StandardLogger().Level)
		logger.Out = logrus.StandardLogger().Out
	}
	client := &Client{
		ID:             id,
		ClusterID:      clusterID,
		ReplicasetUUID: replicasetUUID,
		ReplicasetName: replicasetName,
		NodeType:       nodeType,
		NodeName:       nodeName,
		stream:         stream,
		LastSeen:       time.Now(),
		status:         pb.Status{},
		streamLock:     &sync.Mutex{},
		statusLock:     &sync.Mutex{},
		logger:         logger,
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
	if errMsg := msg.GetErrorMsg(); errMsg != "" {
		return "", fmt.Errorf("Cannot get backup source for client %s: %s", c.NodeName, errMsg)
	}
	return msg.GetBackupSourceMsg(), nil
}

func (c *Client) Status() (pb.Status, error) {
	if err := c.streamSend(&pb.ServerMessage{Type: pb.ServerMessage_GET_STATUS}); err != nil {
		return pb.Status{}, err
	}
	msg, err := c.stream.Recv()
	if err != nil {
		return pb.Status{}, err
	}
	c.logger.Debugf("grpc/server/clients.go GetStatus recv message: %v\n", msg)
	statusMsg := msg.GetStatusMsg()
	c.logger.Debugf("grpc/server/clients.go GetStatus recv message (decoded): %v\n", statusMsg)
	if statusMsg == nil {
		return pb.Status{}, fmt.Errorf("Received nil GetStatus message from client: %s", c.NodeName)
	}

	c.statusLock.Lock()
	c.status = *statusMsg
	c.statusLock.Unlock()
	return *statusMsg, nil
}

func (c *Client) stopBalancer() error {
	err := c.streamSend(&pb.ServerMessage{
		Type:    pb.ServerMessage_STOP_BALANCER,
		Payload: &pb.ServerMessage_EmptyMsg{},
	})
	if err != nil {
		return err
	}
	msg, err := c.stream.Recv()
	if err != nil {
		return err
	}
	if ack := msg.GetAckMsg(); ack != nil {
		return nil
	}
	if errMsg := msg.GetErrorMsg(); errMsg != "" {
		return fmt.Errorf("%s", errMsg)
	}
	return fmt.Errorf("unknown respose type %T", msg)
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

func (c *Client) stopBackup() error {
	msg := &pb.ServerMessage{
		Type:    pb.ServerMessage_STOP_BACKUP,
		Payload: &pb.ServerMessage_StopBackupMsg{},
	}
	if err := c.streamSend(msg); err != nil {
		return err
	}
	if msg, err := c.stream.Recv(); err != nil {
		return err
	} else if ack := msg.GetAckMsg(); ack == nil {
		return fmt.Errorf("Invalid client response to stop backup message. Want 'ack', got %T", msg)
	}
	return nil
}

func (c *Client) SetDBBackupRunning(status bool) {
	c.statusLock.Lock()
	c.status.RunningDBBackUp = status
	c.statusLock.Unlock()
}

func (c *Client) SetOplogTailerRunning(status bool) {
	c.statusLock.Lock()
	c.status.RunningOplogBackup = status
	c.statusLock.Unlock()
}

func (c *Client) IsDBBackupRunning() (status bool) {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()
	return c.status.RunningDBBackUp
}

func (c *Client) IsOplogTailerRunning() (status bool) {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()

	return c.status.RunningOplogBackup
}

func (c *Client) StopOplogTail(ts int64) error {
	c.logger.Debugf("Stopping oplog tail for client: %s, at %d", c.NodeName, ts)
	err := c.streamSend(&pb.ServerMessage{
		Type:    pb.ServerMessage_STOP_OPLOG_TAIL,
		Payload: &pb.ServerMessage_StopOplogTailMsg{StopOplogTailMsg: &pb.StopOplogTail{Ts: ts}},
	})
	if err != nil {
		c.logger.Errorf("Error in client.StopOplogTail stream.Send(...): %s", err)
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
	return c.status.RunningDBBackUp
}

func (c *Client) IsRunningOplogBackup() bool {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()
	return c.status.RunningOplogBackup
}

func (c *Client) streamSend(msg *pb.ServerMessage) error {
	c.streamLock.Lock()
	defer c.streamLock.Unlock()
	return c.stream.Send(msg)
}
