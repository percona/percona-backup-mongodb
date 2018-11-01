package server

import (
	"fmt"
	"sync"
	"time"

	pb "github.com/percona/mongodb-backup/proto/messages"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	ClientAlreadyExistsError = fmt.Errorf("Client ID already registered")
	UnknownClientID          = fmt.Errorf("Unknown client ID")
	timeout                  = 10 * time.Second
)

type Client struct {
	ID        string      `json:"id"`
	NodeType  pb.NodeType `json:"node_type"`
	NodeName  string      `json:"node_name"`
	ClusterID string      `json:"cluster_id"`

	ReplicasetName      string `json:"replicaset_name"`
	ReplicasetUUID      string `json:"replicaset_uuid"`
	ReplicasetVersion   int32  `json:"replicaset_version"`
	isPrimary           bool
	isSecondary         bool
	isTailing           bool
	lastTailedTimestamp int64

	LastCommandSent string    `json:"last_command_sent"`
	LastSeen        time.Time `json:"last_seen"`
	logger          *logrus.Logger

	streamRecvChan chan *pb.ClientMessage

	streamLock *sync.Mutex
	stream     pb.Messages_MessagesChatServer
	statusLock *sync.Mutex
	status     pb.Status
}

// newClient creates a new client in the gRPC server. This client is the one that will handle communications with the
// real client (agent). The method is not exported because only the gRPC server should be able to create a new client.
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
		status: pb.Status{
			RunningDbBackup:    false,
			RunningOplogBackup: false,
			RestoreStatus:      pb.RestoreStatus_RESTORE_STATUS_NOT_RUNNING,
		},
		streamLock:     &sync.Mutex{},
		statusLock:     &sync.Mutex{},
		logger:         logger,
		streamRecvChan: make(chan *pb.ClientMessage),
	}
	go client.handleStreamRecv()
	return client
}

func (c *Client) GetBackupSource() (string, error) {
	if err := c.streamSend(&pb.ServerMessage{
		Payload: &pb.ServerMessage_BackupSourceMsg{BackupSourceMsg: &pb.GetBackupSource{}},
	}); err != nil {
		return "", err
	}
	msg, err := c.streamRecv()
	if err != nil {
		return "", err
	}
	if errMsg := msg.GetErrorMsg(); errMsg != nil {
		return "", fmt.Errorf("Cannot get backup source for client %s: %s", c.NodeName, errMsg)
	}
	return msg.GetBackupSourceMsg().GetSourceClient(), nil
}

func (c *Client) Status() pb.Status {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()
	return c.status
}

func (c *Client) GetStatus() (pb.Status, error) {
	if err := c.streamSend(&pb.ServerMessage{
		Payload: &pb.ServerMessage_GetStatusMsg{GetStatusMsg: &pb.GetStatus{}},
	}); err != nil {
		return pb.Status{}, err
	}
	msg, err := c.streamRecv()
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

func (c *Client) handleStreamRecv() {
	for {
		msg, err := c.stream.Recv()
		if err != nil {
			return
		}
		c.streamRecvChan <- msg
	}
}

func (c *Client) isDBBackupRunning() bool {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()
	return c.status.RunningDbBackup
}

func (c *Client) isOplogBackupRunning() bool {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()
	return c.status.RunningOplogBackup
}

func (c *Client) isOplogTailerRunning() (status bool) {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()

	return c.status.RunningOplogBackup
}

func (c *Client) isRestoreRunning() (status bool) {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()

	return c.status.RestoreStatus > pb.RestoreStatus_RESTORE_STATUS_NOT_RUNNING
}

// listReplicasets will trigger processGetReplicasets on clients connected to a MongoDB instance.
// It makes sense to call it only on config servers since it runs "getShardMap" on MongoDB and that
// only return valid values on config servers.
// By parsing the results of getShardMap, we can know which replicasets are running in the cluster
// and we will use that list to validate we have agents connected to all replicasets, otherwise,
// a backup would be incomplete.
func (c *Client) listReplicasets() ([]string, error) {
	err := c.streamSend(&pb.ServerMessage{Payload: &pb.ServerMessage_ListReplicasets{ListReplicasets: &pb.ListReplicasets{}}})
	if err != nil {
		return nil, err
	}

	response, err := c.streamRecv()
	if err != nil {
		return nil, err
	}

	switch response.Payload.(type) {
	case *pb.ClientMessage_ErrorMsg:
		return nil, fmt.Errorf("Cannot list shards on client %s: %s", c.NodeName, response.GetErrorMsg())
	case *pb.ClientMessage_ReplicasetsMsg:
		shards := response.GetReplicasetsMsg()
		return shards.Replicasets, nil
	}
	return nil, fmt.Errorf("Unkown response type for list Shards message: %T, %+v", response.Payload, response.Payload)
}

func (c *Client) ping() error {
	c.logger.Debug("sending ping")
	err := c.streamSend(&pb.ServerMessage{Payload: &pb.ServerMessage_PingMsg{PingMsg: &pb.Ping{}}})
	if err != nil {
		return errors.Wrap(err, "clients.go -> ping()")
	}

	msg, err := c.streamRecv()
	if err != nil {
		return errors.Wrapf(err, "ping client %s (%s)", c.ID, c.NodeName)
	}

	pongMsg := msg.GetPongMsg()
	c.statusLock.Lock()
	c.NodeType = pongMsg.GetNodeType()
	c.ReplicasetUUID = pongMsg.GetReplicaSetUuid()
	c.ReplicasetVersion = pongMsg.GetReplicaSetVersion()
	c.isPrimary = pongMsg.GetIsPrimary()
	c.isSecondary = pongMsg.GetIsSecondary()
	c.isTailing = pongMsg.GetIsTailing()
	c.lastTailedTimestamp = pongMsg.GetLastTailedTimestamp()
	c.statusLock.Unlock()

	return nil
}

func (c *Client) restoreBackup(msg *pb.RestoreBackup) error {
	outMsg := &pb.ServerMessage{
		Payload: &pb.ServerMessage_RestoreBackupMsg{
			RestoreBackupMsg: &pb.RestoreBackup{
				BackupType:        msg.BackupType,
				SourceType:        msg.SourceType,
				SourceBucket:      msg.SourceBucket,
				DbSourceName:      msg.DbSourceName,
				OplogSourceName:   msg.OplogSourceName,
				CompressionType:   msg.CompressionType,
				Cypher:            msg.Cypher,
				OplogStartTime:    msg.OplogStartTime,
				SkipUsersAndRoles: msg.SkipUsersAndRoles,
			},
		},
	}
	if err := c.streamSend(outMsg); err != nil {
		return err
	}

	response, err := c.streamRecv()
	if err != nil {
		return err
	}
	switch response.Payload.(type) {
	case *pb.ClientMessage_ErrorMsg:
		return fmt.Errorf("Cannot start restore on client %s: %s", c.NodeName, response.GetErrorMsg())
	case *pb.ClientMessage_AckMsg:
		c.setRestoreRunning(true)
		return nil
	}
	return fmt.Errorf("Unkown response type for Restore message: %T, %+v", response.Payload, response.Payload)
}

func (c *Client) setDBBackupRunning(status bool) {
	c.statusLock.Lock()
	c.status.RunningDbBackup = status
	c.statusLock.Unlock()
}

func (c *Client) setOplogTailerRunning(status bool) {
	c.statusLock.Lock()
	c.status.RunningOplogBackup = status
	c.statusLock.Unlock()
}

func (c *Client) setRestoreRunning(status bool) {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()
	if status {
		c.status.RestoreStatus = pb.RestoreStatus_RESTORE_STATUS_RESTORINGDB
		return
	}
	c.status.RestoreStatus = pb.RestoreStatus_RESTORE_STATUS_NOT_RUNNING
}

func (c *Client) startBackup(opts *pb.StartBackup) error {
	msg := &pb.ServerMessage{
		Payload: &pb.ServerMessage_StartBackupMsg{
			StartBackupMsg: &pb.StartBackup{
				BackupType:      opts.BackupType,
				DestinationType: opts.DestinationType,
				DbBackupName:    opts.DbBackupName,
				OplogBackupName: opts.OplogBackupName,
				DestinationDir:  opts.DestinationDir,
				CompressionType: opts.CompressionType,
				Cypher:          opts.Cypher,
				OplogStartTime:  opts.OplogStartTime,
			},
		},
	}
	if err := c.streamSend(msg); err != nil {
		return err
	}
	response, err := c.streamRecv()
	if err != nil {
		return err
	}
	switch response.Payload.(type) {
	case *pb.ClientMessage_AckMsg:
		c.setDBBackupRunning(true)
		c.setOplogTailerRunning(true)
		return nil
	}
	return fmt.Errorf("Invalid client response to start backup message. Want 'ack', got %T", response.Payload)
}

func (c *Client) startBalancer() error {
	err := c.streamSend(&pb.ServerMessage{
		Payload: &pb.ServerMessage_StartBalancerMsg{StartBalancerMsg: &pb.StartBalancer{}},
	})
	if err != nil {
		return err
	}
	msg, err := c.streamRecv()
	if err != nil {
		return err
	}

	switch msg.Payload.(type) {
	case *pb.ClientMessage_ErrorMsg:
		return fmt.Errorf("%s", msg.GetErrorMsg().Message)
	case *pb.ClientMessage_AckMsg:
		return nil
	}
	return fmt.Errorf("unknown respose type %T", msg)
}

func (c *Client) stopBackup() error {
	msg := &pb.ServerMessage{
		Payload: &pb.ServerMessage_CancelBackupMsg{},
	}
	if err := c.streamSend(msg); err != nil {
		return err
	}
	if msg, err := c.streamRecv(); err != nil {
		return err
	} else if ack := msg.GetAckMsg(); ack == nil {
		return fmt.Errorf("Invalid client response to stop backup message. Want 'ack', got %T", msg)
	}
	return nil
}

func (c *Client) stopBalancer() error {
	err := c.streamSend(&pb.ServerMessage{
		Payload: &pb.ServerMessage_StopBalancerMsg{StopBalancerMsg: &pb.StopBalancer{}},
	})
	if err != nil {
		return err
	}
	msg, err := c.streamRecv()
	if err != nil {
		return err
	}

	switch msg.Payload.(type) {
	case *pb.ClientMessage_AckMsg:
		return nil
	case *pb.ClientMessage_ErrorMsg:
		errMsg := msg.GetErrorMsg()
		return fmt.Errorf("%s", errMsg.Message)
	}
	return fmt.Errorf("unknown respose type %T", msg)
}

func (c *Client) stopOplogTail(ts int64) error {
	c.logger.Debugf("Stopping oplog tail for client: %s, at %d", c.NodeName, ts)
	err := c.streamSend(&pb.ServerMessage{
		Payload: &pb.ServerMessage_StopOplogTailMsg{StopOplogTailMsg: &pb.StopOplogTail{Ts: ts}},
	})
	if err != nil {
		c.logger.Errorf("Error in client.StopOplogTail stream.Send(...): %s", err)
	}

	if msg, err := c.streamRecv(); err != nil {
		return err
	} else if ack := msg.GetAckMsg(); ack == nil {
		return fmt.Errorf("Invalid client response to start backup message. Want 'ack', got %T", msg)
	}
	return nil
}

func (c *Client) streamRecv() (*pb.ClientMessage, error) {
	select {
	case msg := <-c.streamRecvChan:
		return msg, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("Timeout reading from the stream")
	}
}

func (c *Client) streamSend(msg *pb.ServerMessage) error {
	c.streamLock.Lock()
	defer c.streamLock.Unlock()
	return c.stream.Send(msg)
}
