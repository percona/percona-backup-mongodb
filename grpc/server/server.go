package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/percona/mongodb-backup/internal/notify"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	EVENT_BACKUP_FINISH = iota
	EVENT_OPLOG_FINISH
)

type MessagesServer struct {
	stopChan chan struct{}
	lock     *sync.Mutex
	clients  map[string]*Client
	// Current backup status
	replicasRunningBackup map[string]bool // Key is ReplicasetUUID
	lastOplogTs           int64           // Timestamp in Unix format
	backupRunning         bool
	oplogBackupRunning    bool
	err                   error
	//
	dbBackupFinishChan    chan interface{}
	oplogBackupFinishChan chan interface{}
	logger                *logrus.Logger
}

func NewMessagesServer(logger *logrus.Logger) *MessagesServer {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.StandardLogger().Level)
		logger.Out = logrus.StandardLogger().Out
	}
	bfc := notify.Start(EVENT_BACKUP_FINISH)
	ofc := notify.Start(EVENT_OPLOG_FINISH)
	messagesServer := &MessagesServer{
		lock:                  &sync.Mutex{},
		clients:               make(map[string]*Client),
		stopChan:              make(chan struct{}),
		dbBackupFinishChan:    bfc,
		oplogBackupFinishChan: ofc,
		replicasRunningBackup: make(map[string]bool),
		logger:                logger,
	}
	return messagesServer
}

func (s *MessagesServer) Stop() {
	close(s.stopChan)
}

func (s *MessagesServer) GetClientsStatus() map[string][]Client {
	replicas := make(map[string][]Client)
	for _, client := range s.clients {
		client.Status()
		if _, ok := replicas[client.ReplicasetUUID]; !ok {
			replicas[client.ReplicasetUUID] = make([]Client, 0)
		}
		replicas[client.ReplicasetUUID] = append(replicas[client.ReplicasetUUID], *client)

	}
	return replicas
}

// IsShardedSystem returns if a system is sharded.
// It check if the Node Type is:
// - Mongos
// - Config Server
// - Shard Server
// or if the ClusterID is not empty because in a sharded system, the cluster id
// is never empty.
func (s *MessagesServer) IsShardedSystem() bool {
	for _, client := range s.clients {
		if client.NodeType == pb.NodeType_MONGOS ||
			client.NodeType == pb.NodeType_MONGOD_CONFIGSVR ||
			client.NodeType == pb.NodeType_MONGOD_SHARDSVR ||
			client.ClusterID != "" {
			return true
		}
	}
	return false
}

func (s *MessagesServer) Clients() map[string]*Client {
	return s.clients
}

func (s *MessagesServer) ClientsByReplicaset() map[string][]*Client {
	replicas := make(map[string][]*Client)
	for _, client := range s.clients {
		if _, ok := replicas[client.ReplicasetUUID]; !ok {
			replicas[client.ReplicasetUUID] = make([]*Client, 0)
		}
		replicas[client.ReplicasetUUID] = append(replicas[client.ReplicasetUUID], client)

	}
	return replicas
}

func (s *MessagesServer) BackupSourceByReplicaset() (map[string]*Client, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	sources := make(map[string]*Client)
	for _, client := range s.clients {
		if _, ok := sources[client.ReplicasetUUID]; !ok {
			if client.NodeType == pb.NodeType_MONGOS {
				continue
			}
			backupSource, err := client.GetBackupSource()
			if err != nil {
				s.logger.Errorf("Cannot get backup source for client %s: %s", client.NodeName, err)
			}
			fmt.Printf("Received backup source from client %s: %s\n", client.NodeName, backupSource)
			if err != nil {
				return nil, fmt.Errorf("Cannot get best client for replicaset %q: %s", client.ReplicasetUUID, err)
			}
			bestClient := s.getClientByNodeName(backupSource)
			if bestClient == nil {
				bestClient = client
				//continue
			}
			sources[client.ReplicasetUUID] = bestClient
		}
	}
	return sources, nil
}

func (s *MessagesServer) ReplicasetsRunningOplogBackup() map[string]*Client {
	replicasets := make(map[string]*Client)

	// use sync.Map?
	for _, client := range s.clients {
		if client.IsOplogTailerRunning() {
			replicasets[client.ReplicasetUUID] = client
		}
	}

	return replicasets
}

func (s *MessagesServer) ReplicasetsRunningDBBackup() map[string]*Client {
	replicasets := make(map[string]*Client)

	for _, client := range s.clients {
		if client.IsDBBackupRunning() {
			replicasets[client.ReplicasetUUID] = client
		}
	}
	return replicasets
}

func (s *MessagesServer) StartBackup(opts *pb.StartBackup) error {
	if s.isBackupRunning() {
		return fmt.Errorf("Backup is already running")
	}
	clients, err := s.BackupSourceByReplicaset()
	if err != nil {
		return errors.Wrapf(err, "Cannot start backup. Cannot find backup source for replicas")
	}

	s.setBackupRunning(true)
	s.setOplogBackupRunning(true)

	for replName, client := range clients {
		s.logger.Printf(">>>> Starting backup for replicaset %q on client %s %s %s", replName, client.ID, client.NodeName, client.NodeType)
		s.replicasRunningBackup[replName] = true
		destinationName := fmt.Sprintf("%s_%s_%s", time.Now().Format("2006-01-02_15.04.05"), client.ReplicasetName, opts.GetDestinationName())
		client.startBackup(&pb.StartBackup{
			BackupType:      opts.GetBackupType(),
			DestinationType: opts.GetDestinationType(),
			DestinationName: destinationName,
			DestinationDir:  opts.GetDestinationDir(),
			CompressionType: opts.GetCompressionType(),
			Cypher:          opts.GetCypher(),
			OplogStartTime:  opts.GetOplogStartTime(),
		})
	}

	return nil
}

func (s *MessagesServer) StopBalancer() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, client := range s.clients {
		if client.NodeType == pb.NodeType_MONGOS {
			return client.stopBalancer()
		}
	}
	return fmt.Errorf("No MongoS server found to stop the balancer")
}

func (s *MessagesServer) cancelBackup() error {
	if !s.isOplogBackupRunning() {
		return fmt.Errorf("Backup is not running")
	}
	s.lock.Lock()
	for _, client := range s.clients {
	}

	return nil
}

func (s *MessagesServer) stopOplogTail() error {
	if !s.isOplogBackupRunning() {
		return fmt.Errorf("Backup is not running")
	}
	var gErr error
	for _, client := range s.clients {
		s.logger.Debugf("Checking if client %s is running the backup: %v", client.NodeName, client.IsOplogTailerRunning())
		if client.IsOplogTailerRunning() {
			s.logger.Debugf("Stopping oplog tail in client %s at %s", client.NodeName, time.Unix(s.lastOplogTs, 0).Format(time.RFC3339))
			err := client.StopOplogTail(s.lastOplogTs)
			if err != nil {
				gErr = errors.Wrapf(err, "client: %s, error: %s", client.NodeName, err)
			}
		}
	}
	s.setBackupRunning(false)

	if gErr != nil {
		return errors.Wrap(gErr, "cannot stop oplog tailer")
	}
	return nil
}

func (s *MessagesServer) WaitBackupFinish() {
	replicasets := s.ReplicasetsRunningDBBackup()
	if len(replicasets) == 0 {
		return
	}
	<-s.dbBackupFinishChan
}

func (s *MessagesServer) WaitOplogBackupFinish() {
	replicasets := s.ReplicasetsRunningOplogBackup()
	if len(replicasets) == 0 {
		return
	}
	<-s.oplogBackupFinishChan
}

// gRPC methods

// MessagesChat is the method exposed by gRPC to stream messages between the server and agents
func (s *MessagesServer) MessagesChat(stream pb.Messages_MessagesChatServer) error {
	msg, err := stream.Recv()
	if err != nil {
		return err
	}

	client, err := s.registerClient(stream, msg)
	if err != nil {
		s.logger.Errorf("Cannot register client: %s", err)
		r := &pb.ServerMessage{
			Type: pb.ServerMessage_ERROR,
			Payload: &pb.ServerMessage_ErrorMsg{
				ErrorMsg: &pb.Error{
					Code:    pb.ErrorType_CLIENT_ALREADY_REGISTERED,
					Message: "",
				},
			},
		}
		err = stream.Send(r)

		return ClientAlreadyExistsError
	}
	s.clients[msg.ClientID] = client
	r := &pb.ServerMessage{
		Type: pb.ServerMessage_REGISTRATION_OK,
	}
	stream.Send(r)

	// Keep the stream open until we receive the stop signal
	// The client will continue processing incomming messages to the stream
	<-s.stopChan

	return nil
}

func (s *MessagesServer) DBBackupFinished(ctx context.Context, msg *pb.DBBackupFinishStatus) (*pb.Ack, error) {
	if !msg.GetOK() {

	}

	client := s.getClientByNodeName(msg.GetClientID())
	if client == nil {
		return nil, fmt.Errorf("Unknown client ID: %s", msg.GetClientID())
	}
	client.SetDBBackupRunning(false)

	replicasets := s.ReplicasetsRunningDBBackup()

	// Keep the last (bigger) oplog timestamp from all clients running the backup.
	// When all clients finish the backup, we will call CloseAt(s.lastOplogTs) on all clients to have a consistent
	// stop time for all oplogs.
	lastOplogTs := msg.GetTs()
	if lastOplogTs > s.lastOplogTs {
		s.lastOplogTs = lastOplogTs
	}

	if len(replicasets) == 0 {
		notify.Post(EVENT_BACKUP_FINISH, time.Now())
	}
	return &pb.Ack{}, nil
}

func (s *MessagesServer) OplogBackupFinished(ctx context.Context, msg *pb.OplogBackupFinishStatus) (*pb.Ack, error) {
	client := s.getClientByNodeName(msg.GetClientID())
	if client == nil {
		return nil, fmt.Errorf("Unknown client ID: %s", msg.GetClientID())
	}
	client.SetOplogTailerRunning(false)

	replicasets := s.ReplicasetsRunningOplogBackup()
	if len(replicasets) == 0 {
		notify.Post(EVENT_OPLOG_FINISH, time.Now())
	}
	return &pb.Ack{}, nil
}

func (s *MessagesServer) setBackupRunning(status bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.backupRunning = status
}

func (s *MessagesServer) isBackupRunning() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.backupRunning
}

func (s *MessagesServer) isOplogBackupRunning() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.oplogBackupRunning
}
func (s *MessagesServer) setOplogBackupRunning(status bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.oplogBackupRunning = status
}

func (s *MessagesServer) getClientByNodeName(name string) *Client {
	for _, client := range s.clients {
		if client.NodeName == name {
			return client
		}
	}
	return nil
}

func (s *MessagesServer) registerClient(stream pb.Messages_MessagesChatServer, msg *pb.ClientMessage) (*Client, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if msg.ClientID == "" {
		return nil, fmt.Errorf("Invalid client ID (empty)")
	}

	if _, exists := s.clients[msg.ClientID]; exists {
		return nil, ClientAlreadyExistsError
	}

	regMsg := msg.GetRegisterMsg()
	if regMsg == nil || regMsg.NodeType == pb.NodeType_UNDEFINED {
		return nil, fmt.Errorf("Node type in register payload cannot be empty")
	}
	s.logger.Debugf("Register msg: %+v", regMsg)
	client := newClient(msg.ClientID, regMsg.ClusterID, regMsg.NodeName, regMsg.ReplicasetID, regMsg.ReplicasetName, regMsg.NodeType, stream, s.logger)
	s.clients[msg.ClientID] = client
	return client, nil
}

func (s *MessagesServer) unregisterClient(client *Client) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, exists := s.clients[client.ID]; !exists {
		return UnknownClientID
	}

	delete(s.clients, client.ID)
	return nil
}
