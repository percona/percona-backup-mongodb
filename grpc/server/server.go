package server

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/percona/mongodb-backup/internal/notify"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"github.com/pkg/errors"
	"github.com/prometheus/common/log"
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
	workDir               string
	lastBackupMetadata    *BackupMetadata
	clientDisconnetedChan chan string
	dbBackupFinishChan    chan interface{}
	oplogBackupFinishChan chan interface{}
	logger                *logrus.Logger
}

func NewMessagesServer(workDir string, logger *logrus.Logger) *MessagesServer {
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
		clientDisconnetedChan: make(chan string),
		stopChan:              make(chan struct{}),
		dbBackupFinishChan:    bfc,
		oplogBackupFinishChan: ofc,
		replicasRunningBackup: make(map[string]bool),
		workDir:               workDir,
		logger:                logger,
	}

	go messagesServer.handleClientDisconnection()

	return messagesServer
}

func (s *MessagesServer) handleClientDisconnection() {
	for {
		clientID := <-s.clientDisconnetedChan
		log.Infof("Client %s has been disconnected", clientID)
		s.unregisterClient(clientID)
		// TODO: Check if backup is running to stop it or to tell a different client in the
		// same replicaset to start a new backup
	}
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

func (s *MessagesServer) Clients() map[string]Client {
	s.lock.Lock()
	defer s.lock.Unlock()

	c := make(map[string]Client)
	for id, client := range s.clients {
		c[id] = *client
	}
	return c
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

	s.lastBackupMetadata = NewBackupMetadata()

	s.lastBackupMetadata.StartTs = time.Now()
	s.lastBackupMetadata.Replicasets = make(map[string]ReplicasetMetadata)
	s.lastBackupMetadata.BackupType = opts.GetBackupType()
	s.lastBackupMetadata.DestinationType = opts.GetDestinationType()
	s.lastBackupMetadata.DestinationDir = opts.GetDestinationDir()
	s.lastBackupMetadata.CompressionType = opts.GetCompressionType()
	s.lastBackupMetadata.Cypher = opts.GetCypher()

	clients, err := s.BackupSourceByReplicaset()
	if err != nil {
		return errors.Wrapf(err, "Cannot start backup. Cannot find backup source for replicas")
	}

	s.reset()
	s.setBackupRunning(true)
	s.setOplogBackupRunning(true)

	for replName, client := range clients {
		s.logger.Printf("Starting backup for replicaset %q on client %s %s %s", replName, client.ID, client.NodeName, client.NodeType)
		s.replicasRunningBackup[replName] = true
		destinationName := fmt.Sprintf("%s_%s_%s", s.lastBackupMetadata.StartTs.Format("2006-01-02_15.04.05"), client.ReplicasetName, opts.GetDestinationName())

		s.lastBackupMetadata.AddReplicaset(client.ReplicasetName, client.ReplicasetUUID, destinationName)

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

	metadataFilename := path.Join(s.workDir, fmt.Sprintf("%s.json", s.lastBackupMetadata.StartTs.Format("2006-01-02_15.04.05")))
	err = s.lastBackupMetadata.WriteMetadataToFile(metadataFilename)
	if err != nil {
		log.Warn("Cannot write metadata file %s: %s", metadataFilename, err)
	}
	return nil
}

// StartBalancer restarts the balancer if this is a sharded system
func (s *MessagesServer) StartBalancer() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, client := range s.clients {
		if client.NodeType == pb.NodeType_MONGOS {
			return client.startBalancer()
		}
	}
	// This is not a sharded system. There is nothing to do.
	return nil
}

// StopBalancer stops the balancer if this is a sharded system
func (s *MessagesServer) StopBalancer() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, client := range s.clients {
		if client.NodeType == pb.NodeType_MONGOS {
			return client.stopBalancer()
		}
	}
	// This is not a sharded system. There is nothing to do.
	return nil
}

func (s *MessagesServer) cancelBackup() error {
	if !s.isOplogBackupRunning() {
		return fmt.Errorf("Backup is not running")
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	var gerr error
	for _, client := range s.clients {
		if err := client.stopBackup(); err != nil {
			gerr = errors.Wrapf(err, "cannot stop backup on %s", client.ID)
		}
	}
	return gerr
}

// StopOplogTail calls ever agent StopOplogTail(ts) method using the last oplog timestamp reported by the clients
// when they call DBBackupFinished after mongodump finish on each client. That way s.lastOplogTs has the last
// timestamp of the slowest backup
func (s *MessagesServer) StopOplogTail() error {
	if !s.isOplogBackupRunning() {
		return fmt.Errorf("Backup is not running")
	}
	// This should never happen. We get the last oplog timestamp when agents call DBBackupFinished
	if s.lastOplogTs == 0 {
		log.Errorf("Trying to stop the oplog tailer but last oplog timestamp is 0. Using current timestamp")
		s.lastOplogTs = time.Now().Unix()
	}

	var gErr error
	for _, client := range s.clients {
		s.logger.Debugf("Checking if client %s is running the backup: %v", client.NodeName, client.IsOplogTailerRunning())
		if client.IsOplogTailerRunning() {
			s.logger.Debugf("Stopping oplog tail in client %s at %s", client.NodeName, time.Unix(s.lastOplogTs, 0).Format(time.RFC3339))
			err := client.StopOplogTail(s.lastOplogTs)
			if err != nil {
				gErr = errors.Wrapf(gErr, "client: %s, error: %s", client.NodeName, err)
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

// ---------------------------------------------------------------------------------------------------------------------
// gRPC methods
// ---------------------------------------------------------------------------------------------------------------------

// MessagesChat is the method exposed by gRPC to stream messages between the server and agents
func (s *MessagesServer) MessagesChat(stream pb.Messages_MessagesChatServer) error {
	msg, err := stream.Recv()
	if err != nil {
		return err
	}

	log.Debugf("Registering new client: %s", msg.GetClientID())
	client, err := s.registerClient(stream, msg)
	if err != nil {
		s.logger.Errorf("Cannot register client: %s", err)
		r := &pb.ServerMessage{
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
		Payload: &pb.ServerMessage_AckMsg{AckMsg: &pb.Ack{}},
	}

	if err := stream.Send(r); err != nil {
		return err
	}

	// Keep the stream open
	<-stream.Context().Done()

	return nil
}

// DBBackupFinished process backup finished message from clients.
// After the mongodump call finishes, clients should call this method to inform the event to the server
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

// OplogBackupFinished process oplog tailer finished message from clients.
// After the the oplog tailer has been closed on clients, clients should call this method to inform the event to the server
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

// ---------------------------------------------------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------------------------------------------------

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

func (s *MessagesServer) reset() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.lastOplogTs = 0
	s.backupRunning = false
	s.oplogBackupRunning = false
	s.err = nil
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

	if client, exists := s.clients[msg.ClientID]; exists {
		if err := client.Ping(); err != nil {
			s.unregisterClient(client.ID) // Since we already know the client ID is valid, it will never return an error
		} else {
			return nil, ClientAlreadyExistsError
		}
	}

	regMsg := msg.GetRegisterMsg()
	if regMsg == nil || regMsg.NodeType == pb.NodeType_UNDEFINED {
		return nil, fmt.Errorf("Node type in register payload cannot be empty")
	}
	s.logger.Debugf("Register msg: %+v", regMsg)
	client := newClient(msg.ClientID, regMsg.ClusterID, regMsg.NodeName, regMsg.ReplicasetID, regMsg.ReplicasetName,
		regMsg.NodeType, stream, s.logger, s.clientDisconnetedChan)
	s.clients[msg.ClientID] = client
	return client, nil
}

func (s *MessagesServer) unregisterClient(id string) error {
	log.Infof("Unregistering client %s", id)
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, exists := s.clients[id]; !exists {
		return UnknownClientID
	}

	delete(s.clients, id)
	return nil
}
