package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/percona/mongodb-backup/internal/notify"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	EVENT_BACKUP_FINISH = iota
	EVENT_OPLOG_FINISH
)

type MessagesServer struct {
	stopChan              chan struct{}
	lock                  *sync.Mutex
	clients               map[string]*Client
	replicasRunningBackup map[string]bool
	backupRunning         bool
	dbBackupFinishChan    chan interface{}
	oplogBackupFinishChan chan interface{}
}

func NewMessagesServer() *MessagesServer {
	bfc := notify.Start(EVENT_BACKUP_FINISH)
	ofc := notify.Start(EVENT_OPLOG_FINISH)
	messagesServer := &MessagesServer{
		lock:                  &sync.Mutex{},
		clients:               make(map[string]*Client),
		stopChan:              make(chan struct{}),
		dbBackupFinishChan:    bfc,
		oplogBackupFinishChan: ofc,
	}
	return messagesServer
}

func (s *MessagesServer) GetClientsStatus() {
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

// MessagesChat is the method exposed by gRPC to stream messages between the server and agents
func (s *MessagesServer) MessagesChat(stream pb.Messages_MessagesChatServer) error {
	msg, err := stream.Recv()
	if err != nil {
		return err
	}

	client, err := s.registerClient(stream, msg)
	if err != nil {
		log.Errorf("Cannot register client: %s", err)
		r := &pb.ServerMessage{
			Type: pb.ServerMessage_ERROR,
			Payload: &pb.ServerMessage_ErrorMsg{
				ErrorMsg: &pb.Error{
					Code:    pb.ErrorType_CLIENT_ALREADY_REGISTERED,
					Message: "",
				},
			},
		}
		stream.Send(r)
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
	client := s.getClientByNodeName(msg.GetClientID())
	client.lock.Lock()
	client.Status.DBBackUpRunning = false
	client.lock.Unlock()

	replicasets := s.ReplicasetsRunningDBBackup()
	if len(replicasets) == 0 {
		notify.Post(EVENT_BACKUP_FINISH, time.Now())
	}
	return &pb.Ack{}, nil
}

func (s *MessagesServer) OplogBackupFinished(ctx context.Context, msg *pb.OplogBackupFinishStatus) (*pb.Ack, error) {
	log.Println("???????????????????????????????????????????????????????????????????????????????")
	client := s.getClientByNodeName(msg.GetClientID())
	fmt.Println("locking 2")
	client.lock.Lock()
	client.Status.OplogBackupRunning = false
	client.lock.Unlock()
	fmt.Println("unlocked")

	replicasets := s.ReplicasetsRunningOplogBackup()
	if len(replicasets) == 0 {
		notify.Post(EVENT_OPLOG_FINISH, time.Now())
	}
	close(s.oplogBackupFinishChan)
	return &pb.Ack{}, nil
}

func (s *MessagesServer) Stop() {
	close(s.stopChan)
}

func (s *MessagesServer) Clients() map[string]*Client {
	return s.clients
}

func (s *MessagesServer) ClientsByReplicaset() map[string][]*Client {
	replicas := make(map[string][]*Client)
	for _, client := range s.clients {
		if _, ok := replicas[client.ReplicasetID]; !ok {
			replicas[client.ReplicasetID] = make([]*Client, 0)
		}
		replicas[client.ReplicasetID] = append(replicas[client.ReplicasetID], client)

	}
	return replicas
}

func (s *MessagesServer) BackupSourceByReplicaset() (map[string]*Client, error) {
	sources := make(map[string]*Client)

	for _, client := range s.clients {
		if _, ok := sources[client.ReplicasetID]; !ok {
			backupSource, err := client.GetBackupSource()
			if err != nil {
				return nil, fmt.Errorf("Cannot get best client for replicaset %q: %s", client.ReplicasetID, err)
			}
			bestClient := s.getClientByNodeName(backupSource)
			if bestClient == nil {
				return nil, fmt.Errorf("Cannot get best client for replicaset %q. No such client %q", client.ReplicasetID, bestClient)
			}
			sources[client.ReplicasetID] = bestClient
		}
	}

	return sources, nil
}

func (s *MessagesServer) ReplicasetsRunningOplogBackup() map[string]*Client {
	replicasets := make(map[string]*Client)

	// use sync.Map?
	s.lock.Lock()
	for _, client := range s.clients {
		client.GetStatus()
		if client.Status.OplogBackupRunning {
			replicasets[client.ReplicasetID] = client
		}
	}
	s.lock.Unlock()

	return replicasets
}

func (s *MessagesServer) ReplicasetsRunningDBBackup() map[string]*Client {
	replicasets := make(map[string]*Client)

	s.lock.Lock()
	for _, client := range s.clients {
		client.GetStatus()
		if client.Status.DBBackUpRunning {
			replicasets[client.ReplicasetID] = client
		}
	}
	s.lock.Unlock()

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
	for replName, client := range clients {
		s.replicasRunningBackup[replName] = true
		client.StartBackup(&pb.StartBackup{
			BackupType:      opts.GetBackupType(),
			DestinationType: opts.GetDestinationType(),
			DestinationName: opts.GetDestinationName(),
			DestinationDir:  opts.GetDestinationDir(),
			CompressionType: opts.GetCompressionType(),
			Cypher:          opts.GetCypher(),
			OplogStartTime:  opts.GetOplogStartTime(),
		})
	}

	return nil
}

func (s *MessagesServer) StopOplogTail() error {
	if !s.isOplogBackupRunning() {
		return fmt.Errorf("Backup is not running")
	}
	clients, err := s.BackupSourceByReplicaset()
	if err != nil {
		return errors.Wrapf(err, "Cannot stop oplog tailers. Cannot find backup source for replicas")
	}
	for _, client := range clients {
		if client.IsOplogBackupRunning() {
			log.Printf("Stopping oplog tail in client %s", client.NodeName)
			err := client.StopOplogTail()
			if err != nil {
				log.Errorf("Error calling client.StopOplogTail: %s", err)
			}
			return err
		}
	}

	s.setBackupRunning(false)

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
	repl := s.ReplicasetsRunningOplogBackup()
	if len(repl) > 0 {
		return true
	}
	return false
}

func (s *MessagesServer) getClientByNodeName(name string) *Client {
	fmt.Println("locking 1")
	s.lock.Lock()
	defer fmt.Println("unlocking 1")
	fmt.Println("locked 1")
	defer s.lock.Unlock()
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
	client := NewClient(msg.ClientID, regMsg.ClusterID, regMsg.NodeName, regMsg.ReplicasetID, regMsg.ReplicasetName, regMsg.NodeType, stream)
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
