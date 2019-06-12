package server

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/olebedev/emitter"
	"github.com/percona/percona-backup-mongodb/internal/notify"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
	"github.com/pkg/errors"
	"github.com/prometheus/common/log"
	"github.com/sirupsen/logrus"
)

const (
	EventBackupFinish = iota
	EventOplogFinish
	EventRestoreFinish

	// If this list is updated, please also update the isValidEvent and eventName functions
	BackupFinishedEvent      = 0
	BackupStartedEvent       = 1
	BeforeBalancerStartEvent = 2
	AfterBalancerStartEvent  = 3
	BeforeBalancerStopEvent  = 4
	AfterBalancerStopEvent   = 5

	WaitForever = -1

	logBufferSize = 500
)

type backupStatus struct {
	// the balancer status before starting a backup/restore.
	// After backup/restore has been completed, we must return the balancer status to its original state
	balancerStatus     *pb.BalancerStatus
	lastBackupMetadata *BackupMetadata
	//	lastOplogTs        int64 // Timestamp in Unix format
	// The name lastBackupErrors is plural because we are concatenating errors using the multierror pkg
	lastBackupErrors      error
	replicasRunningBackup map[string]bool // Key is ReplicasetUUID
	backupRunning         bool
	oplogBackupRunning    bool
	sources               map[string]*Client // clients that ran the last backup
}

type MessagesServer struct {
	workDir               string
	replicasRunningBackup map[string]bool // Key is ReplicasetUUID
	restoreRunning        bool
	clientLoggingEnabled  bool

	backupStatusLock *sync.Mutex
	backupStatus     backupStatus

	clientsLock *sync.Mutex
	clients     map[string]*Client

	logger *logrus.Logger

	eventsNotifier *emitter.Emitter

	clientDisconnetedChan chan string
	stopChan              chan struct{}

	// Events notification channels
	oplogBackupFinishChan chan interface{}
	restoreFinishChan     chan interface{}

	clientsLogChan chan *pb.LogEntry
}

type StorageEntry struct {
	MatchClients  []string
	DifferClients []string
	StorageInfo   *pb.StorageInfo
}

type RestoreSource struct {
	Client *Client
	Host   string
	Port   string
}

func NewMessagesServer(workDir string) *MessagesServer {
	messagesServer := newMessagesServer(workDir, nil)
	return messagesServer
}

func NewMessagesServerWithClientLogging(workDir string, logger *logrus.Logger) *MessagesServer {
	messagesServer := newMessagesServer(workDir, logger)
	messagesServer.clientLoggingEnabled = true
	return messagesServer
}

func newMessagesServer(workDir string, logger *logrus.Logger) *MessagesServer {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.StandardLogger().Level)
		logger.Out = logrus.StandardLogger().Out
	}

	if workDir == "" {
		workDir = "."
	}

	messagesServer := &MessagesServer{
		clientsLock:      &sync.Mutex{},
		backupStatusLock: &sync.Mutex{},

		clients:               make(map[string]*Client),
		clientDisconnetedChan: make(chan string),
		stopChan:              make(chan struct{}),
		clientsLogChan:        make(chan *pb.LogEntry, logBufferSize),
		//
		oplogBackupFinishChan: notify.Start(EventOplogFinish),
		restoreFinishChan:     notify.Start(EventRestoreFinish),

		eventsNotifier: &emitter.Emitter{},

		replicasRunningBackup: make(map[string]bool), // Key is ReplicasetUUID
		backupStatus: backupStatus{
			lastBackupErrors:      nil,
			replicasRunningBackup: make(map[string]bool),
			lastBackupMetadata:    NewBackupMetadata(&pb.StartBackup{}),
		},
		workDir: workDir,
		logger:  logger,
	}
	return messagesServer
}

func (s *MessagesServer) BackupSourceNameByReplicaset() (map[string]string, error) {
	s.clientsLock.Lock()
	defer s.clientsLock.Unlock()

	sources := make(map[string]string)
	for _, client := range s.clients {
		if _, ok := sources[client.ReplicasetName]; !ok {
			if client.NodeType == pb.NodeType_NODE_TYPE_MONGOS {
				continue
			}
			backupSource, err := client.GetBackupSource()
			if err != nil {
				s.logger.Errorf("Cannot get backup source for client %s: %s", client.NodeName, err)
			}
			sources[client.ReplicasetName] = backupSource
		}
	}
	return sources, nil
}

func (s *MessagesServer) RestoreSourcesByReplicaset(bm *pb.BackupMetadata, storageName string) (
	map[string]RestoreSource, error) {
	sources := make(map[string]RestoreSource)
	wga := &sync.WaitGroup{}
	wgb := &sync.WaitGroup{}
	ch := make(chan pb.CanRestoreBackupResponse)

	wga.Add(1)
	go func() {
		for resp := range ch {
			if !resp.CanRestore {
				continue
			}
			_, ok := sources[resp.Replicaset]
			if !ok {
				sources[resp.Replicaset] = RestoreSource{
					Client: s.getClientByID(resp.ClientId),
				}
			}
			if resp.IsPrimary {
				s := sources[resp.Replicaset]
				s.Host = resp.Host
				s.Port = resp.Port
				sources[resp.Replicaset] = s
			}
		}
		wga.Done()
	}()

	s.clientsLock.Lock()
	for replicasetName, replicasetMetaData := range bm.Replicasets {
		for _, client := range s.clients {
			if client.NodeType == pb.NodeType_NODE_TYPE_MONGOS {
				continue
			}
			if client.ReplicasetName != replicasetName {
				continue
			}

			// make a copy to avoid dataraces
			c := client
			bmType := bm.BackupType
			backupName := replicasetMetaData.DbBackupName

			wgb.Add(1)
			go func() {
				defer wgb.Done()
				resp, err := c.CanRestoreBackup(bmType, backupName, storageName)
				if err != nil {
					return
				}
				ch <- resp
			}()
		}
	}
	s.clientsLock.Unlock()

	wgb.Wait()
	close(ch)
	wga.Wait()

	if len(sources) != len(bm.Replicasets) {
		var err error
		for replicasetName, replicasetMetaData := range bm.Replicasets {
			if _, ok := sources[replicasetName]; !ok {
				err = multierror.Append(err,
					fmt.Errorf("there are no clients connected to replicaset %s that can restore %s from %s",
						replicasetName,
						replicasetMetaData.DbBackupName,
						storageName,
					),
				)
			}
		}
		return nil, err
	}
	return sources, nil
}

func (s *MessagesServer) BackupSourceByReplicaset() (map[string]*Client, error) {
	sources := make(map[string]*Client)

	s.clientsLock.Lock()
	defer s.clientsLock.Unlock()

	for _, client := range s.clients {
		if _, ok := sources[client.ReplicasetName]; !ok {
			if client.NodeType == pb.NodeType_NODE_TYPE_MONGOS {
				continue
			}
			backupSource, err := client.GetBackupSource()
			if err != nil {
				s.logger.Errorf("Cannot get backup source for client %s: %s", client.NodeName, err)
				continue
			}
			bestClient := s.getClientByNodeName(backupSource)
			if bestClient != nil {
				sources[client.ReplicasetName] = bestClient
			}
		}
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("cannot get best client")
	}
	return sources, nil
}

func (s *MessagesServer) Clients() map[string]Client {
	s.clientsLock.Lock()
	defer s.clientsLock.Unlock()

	c := make(map[string]Client)
	for id, client := range s.clients {
		c[id] = *client
	}
	return c
}

func (s *MessagesServer) ClientsByReplicaset() map[string][]Client {
	replicas := make(map[string][]Client)
	for _, client := range s.clients {
		if client.ReplicasetName == "" { // mongos?
			continue
		}
		if _, ok := replicas[client.ReplicasetName]; !ok {
			replicas[client.ReplicasetName] = make([]Client, 0)
		}
		replicas[client.ReplicasetName] = append(replicas[client.ReplicasetName], *client)

	}

	return replicas
}

func (s *MessagesServer) LastOplogTs() int64 {
	return s.backupStatus.lastBackupMetadata.LastOplogTs()
}

func (s *MessagesServer) ListStorages() (map[string]StorageEntry, error) {
	return s.listStorages()
}

func (s *MessagesServer) listStorages() (map[string]StorageEntry, error) {
	stgs := make(map[string][]*pb.StorageInfo)
	// Get all storages from all clients.
	// At the end of this loop, stgs is a map where the key is the client id and the value is an
	// array of all storages that client has defined.
	type resp struct {
		id     string
		ssInfo []*pb.StorageInfo
	}
	var errs error
	l := &sync.Mutex{}

	for lid, lc := range s.clients {
		id := lid
		c := lc
		ssInfo, err := c.GetStoragesInfo()
		if err != nil {
			errs = multierror.Append(errs, err)
		}
		stgs[id] = ssInfo
	}

	if errs != nil {
		return nil, errs
	}

	// Group storages by name and build two lists:
	// 1. Clients where the storages definitions matches
	// 2. Clients where the storages definitions are different
	// The list is sorted only to make it easier to test
	ss := make(map[string]StorageEntry)
	for clientID, storages := range stgs {
		for _, info := range storages {
			stg, ok := ss[info.Name]
			if !ok {
				ss[info.Name] = StorageEntry{
					MatchClients:  []string{clientID},
					DifferClients: []string{},
					StorageInfo:   info,
				}
				continue
			}
			if reflect.DeepEqual(info, stg.StorageInfo) {
				stg.MatchClients = append(stg.MatchClients, clientID)
				sort.Strings(stg.MatchClients)
			} else {
				stg.DifferClients = append(stg.DifferClients, clientID)
				sort.Strings(stg.DifferClients)
			}
			ss[info.Name] = stg
		}
	}
	return ss, nil
}

func (s *MessagesServer) RefreshClients() error {
	s.clientsLock.Lock()
	defer s.clientsLock.Unlock()

	for _, client := range s.clients {
		if err := client.ping(); err != nil {
			return errors.Wrapf(err, "cannot refresh clients list while pinging client %s (%s)", client.ID, client.NodeName)
		}
	}
	return nil
}

// IsShardedSystem returns if a system is sharded.
// It check if the Node Type is:
// - Mongos
// - Config Server
// - Shard Server
// or if the ClusterID is not empty because in a sharded system, the cluster id is never empty.
func (s *MessagesServer) IsShardedSystem() bool {
	for _, client := range s.clients {
		if client.NodeType == pb.NodeType_NODE_TYPE_MONGOS ||
			client.NodeType == pb.NodeType_NODE_TYPE_MONGOD_CONFIGSVR ||
			client.NodeType == pb.NodeType_NODE_TYPE_MONGOD_SHARDSVR ||
			client.ClusterID != "" {
			return true
		}
	}
	return false
}

func (s *MessagesServer) LastBackupErrors() error {
	return s.backupStatus.lastBackupErrors
}

func (s *MessagesServer) LastBackupMetadata() *BackupMetadata {
	return s.backupStatus.lastBackupMetadata
}

func (s *MessagesServer) ListBackups() (map[string]pb.BackupMetadata, error) {
	files, err := ioutil.ReadDir(s.workDir)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot list workdir %q backup filenames", s.workDir)
	}

	backups := make(map[string]pb.BackupMetadata)

	// mongoDBVersion, err := s.getMongoDBVersion()
	// if err != nil {
	// 	s.logger.Info("get db version: " + err.Error())
	// }

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		filename := filepath.Join(s.workDir, file.Name())
		bm, err := LoadMetadataFromFile(filename)
		if err != nil {
			return nil, fmt.Errorf("invalid backup metadata file %s: %s", filename, err)
		}

		backups[file.Name()] = *bm.Metadata()

		// TODO: Why do we need the for the K8s operator?
		// for replName, source := range s.clients {
		// 	s.logger.Info("Range replicasets for replset " + replName + "and s.replsname: " + source.ReplicasetName)
		// 	for bmReplName, metadata := range backups[file.Name()].Replicasets {
		// 		s.logger.Info("bmRepName:" + bmReplName + ", meta:" + metadata.ClusterId)
		// 		if bmReplName == source.ReplicasetName {
		// 			msg := &pb.RestoreBackupCheck{
		// 				BackupType:        backups[file.Name()].BackupType,
		// 				DbSourceName:      metadata.DbBackupName,
		// 				OplogSourceName:   metadata.OplogBackupName,
		// 				CompressionType:   backups[file.Name()].CompressionType,
		// 				Cypher:            backups[file.Name()].Cypher,
		// 				SkipUsersAndRoles: false,
		// 				Host:              "",
		// 				Port:              "",
		// 				StorageName:       backups[file.Name()].StorageName,
		// 				MongodbVersion:    mongoDBVersion,
		// 			}
		// 			s.logger.Info("Check if can restore")
		// 			resp, err := source.RestoreBackupCheck(msg, metadata.DbBackupName, backups[file.Name()].StorageName)
		// 			if err != nil {
		// 				s.logger.Info("canrestore request:" + err.Error())
		// 				continue
		// 			}

		// 			newBackup := backups[file.Name()]
		// 			if resp.CanRestore && time.Now().Unix()-newBackup.StartTs > 30 {
		// 				newBackup.EndTs = int64(1)
		// 			}
		// 			backups[file.Name()] = newBackup
		// 		}
		// 	}
		// }
	}
	s.restoreRunning = false
	return backups, nil
}

func (s *MessagesServer) ReplicasetsRunningDBBackup() map[string]*Client {
	replicasets := make(map[string]*Client)

	s.clientsLock.Lock()
	for _, client := range s.clients {
		if client.isDBBackupRunning() {
			replicasets[client.ReplicasetName] = client
		}
	}
	s.clientsLock.Unlock()
	return replicasets
}

func (s *MessagesServer) ReplicasetsRunningOplogBackup() map[string]*Client {
	replicasets := make(map[string]*Client)

	// use sync.Map?
	for _, client := range s.clients {
		if client.isOplogTailerRunning() {
			replicasets[client.ReplicasetName] = client
		}
	}

	return replicasets
}

func (s *MessagesServer) ReplicasetsRunningRestore() map[string]*Client {
	s.clientsLock.Lock()
	defer s.clientsLock.Unlock()

	replicasets := make(map[string]*Client)

	// use sync.Map?
	for _, client := range s.clients {
		if client.isRestoreRunning() {
			replicasets[client.ReplicasetName] = client
		}
	}

	return replicasets
}

// RestoreBackupFromMetadataFile is just a wrappwe around RestoreBackUp that receives a metadata filename
// loads and parse it and then call RestoreBackUp
func (s *MessagesServer) RestoreBackupFromMetadataFile(filename, storageName string, skipUsersAndRoles bool) error {
	filename = filepath.Join(s.workDir, filename)
	bm, err := LoadMetadataFromFile(filename)
	if err != nil {
		log.Errorf("invalid backup metadata file %s: %s", filename, err)
		return fmt.Errorf("invalid backup metadata file %s: %s", filename, err)
	}

	return s.RestoreBackUp(bm.Metadata(), storageName, skipUsersAndRoles)
}

// RestoreBackUp will run a restore on each client, using the provided backup metadata to choose the source for each
// replicaset.
func (s *MessagesServer) RestoreBackUp(bm *pb.BackupMetadata, storageName string, skipUsersAndRoles bool) error {
	clients, err := s.RestoreSourcesByReplicaset(bm, storageName)
	if err != nil {
		log.Errorf("cannot start backup restore. Cannot find backup source for replicas: %v", err)
		return errors.Wrapf(err, "cannot start backup restore. Cannot find backup source for replicas")
	}

	if s.isBackupRunning() {
		log.Errorf("cannot start a restore while a backup is running")
		return fmt.Errorf("cannot start a restore while a backup is running")
	}
	if s.isRestoreRunning() {
		log.Errorf("cannot start a restore while another restore is still running")
		//return fmt.Errorf("cannot start a restore while another restore is still running")
	}

	stgs, err := s.listStorages()
	if err != nil {
		log.Errorf("cannot get the storages list: %s", err)
		return fmt.Errorf("cannot get the storages list: %s", err)
	}

	stg, ok := stgs[storageName]
	if !ok {
		log.Errorf("invalid storage %q", storageName)
		return fmt.Errorf("invalid storage %q", storageName)
	}
	if !stg.StorageInfo.Valid {
		log.Errorf("storage %q is invalid", storageName)
		return fmt.Errorf("storage %q is invalid", storageName)
	}
	s.reset()
	s.setRestoreRunning(true)

	// Ping will also update the status and if it is primary or secondary
	for _, source := range clients {
		if err := source.Client.ping(); err != nil {
			s.setRestoreRunning(false)
			return errors.Wrapf(err, "error while sending ping to client %s", source.Client.ID)
		}
	}

	mongoDBVersion, err := s.getMongoDBVersion()
	if err != nil {
		s.setRestoreRunning(false)
		return err
	}

	for replName, source := range clients {
		s.logger.Infof("Starting restore for replicaset %q on client %s %s %s",
			replName,
			source.Client.ID,
			source.Client.NodeName,
			source.Client.NodeType,
		)
		s.replicasRunningBackup[replName] = true
		s.logger.Infof("Starting restore for replicaset %q on client %s %s %s",
			replName,
			source.Client.ID,
			source.Client.NodeName,
			source.Client.NodeType,
		)
		s.backupStatus.replicasRunningBackup[replName] = true
		for bmReplName, metadata := range bm.Replicasets {
			if bmReplName == replName {
				msg := &pb.RestoreBackup{
					BackupType:        bm.BackupType,
					DbSourceName:      metadata.DbBackupName,
					OplogSourceName:   metadata.OplogBackupName,
					CompressionType:   bm.CompressionType,
					Cypher:            bm.Cypher,
					SkipUsersAndRoles: skipUsersAndRoles,
					Host:              source.Host,
					Port:              source.Port,
					StorageName:       storageName,
					MongodbVersion:    mongoDBVersion,
				}
				if err := source.Client.restoreBackup(msg); err != nil {
					s.setRestoreRunning(false)
					log.Errorf("cannot send restore backup message to client %v", err)
					return errors.Wrapf(err, "cannot send restore backup message to client %s", source.Client.ID)
				}
			}
		}
	}

	return nil
}

// TODO Create an API StartBackup message instead of using pb.StartBackup
// For example, we don't need DBBackupName & OplogBackupName and having them,
// here if we use pb.StartBackup message, leads to confusions
func (s *MessagesServer) StartBackup(opts *pb.StartBackup) error {
	if s.isBackupRunning() {
		return fmt.Errorf("cannot start a backup while another backup is still running")
	}
	if s.isRestoreRunning() {
		return fmt.Errorf("cannot start a backup while a restore is still running")
	}

	if err := s.ValidateReplicasetAgents(); err != nil {
		return errors.Wrap(err, "cannot start a backup while not all MongoDB instances have a backup agent")
	}

	stgs, err := s.listStorages()
	if err != nil {
		return fmt.Errorf("cannot get the storages list: %s", err)
	}

	stg, ok := stgs[opts.GetStorageName()]
	if !ok {
		return fmt.Errorf("invalid storage %q", opts.GetStorageName())
	}
	if !stg.StorageInfo.Valid {
		return fmt.Errorf("storage %q is invalid", opts.GetStorageName())
	}

	ext := getFileExtension(opts.CompressionType, opts.Cypher)

	s.backupStatus.lastBackupMetadata = NewBackupMetadata(opts)

	if err := s.getBalancerStatus(); err != nil {
		return errors.Wrap(err, "cannot save the current balancer status")
	}

	cmdLineOpts, err := s.AllServersCmdLineOpts()
	if err != nil {
		return errors.Wrap(err, "cannot Get Cmd Line Opts")
	}
	s.backupStatus.lastBackupMetadata.metadata.Servers = cmdLineOpts

	if err := s.RefreshClients(); err != nil {
		return errors.Wrapf(err, "cannot refresh clients state for backup")
	}

	s.backupStatus.sources, err = s.BackupSourceByReplicaset()
	if err != nil {
		return errors.Wrapf(err, "cannot start backup. Cannot find backup source for replicas")
	}

	mongoDBVersion, err := s.getMongoDBVersion()
	if err != nil {
		return err
	}
	s.backupStatus.lastBackupMetadata.metadata.MongodbVersion = mongoDBVersion

	s.reset()
	s.setBackupRunning(true)
	s.setOplogBackupRunning(true)

	if err := s.triggerEvent(BackupStartedEvent, time.Now()); err != nil {
		s.logger.Errorf("cannot trigger event %s: %s", eventName(BackupStartedEvent), err)
	}

	for replName, client := range s.backupStatus.sources {
		s.logger.Infof("Starting backup for replicaset %q on client %s %s %s",
			replName,
			client.ID,
			client.NodeName,
			client.NodeType,
		)
		s.replicasRunningBackup[replName] = true
		s.logger.Infof("Starting backup for replicaset %q on client %s %s %s",
			replName,
			client.ID,
			client.NodeName,
			client.NodeType,
		)
		s.backupStatus.replicasRunningBackup[replName] = true
		if client.isPrimary {
			s.logger.Warnf("Warning! Client %s is the primary", client.ID)
		}

		dbBackupName := fmt.Sprintf("%s_%s.dump%s", opts.NamePrefix, client.ReplicasetName, ext)
		oplogBackupName := fmt.Sprintf("%s_%s.oplog%s", opts.NamePrefix, client.ReplicasetName, ext)

		err := s.backupStatus.lastBackupMetadata.AddReplicaset(client.ClusterID,
			client.ReplicasetName,
			client.ReplicasetUUID,
			dbBackupName,
			oplogBackupName,
		)
		if err != nil {
			s.setBackupRunning(false)
			s.setOplogBackupRunning(false)
			return errors.Wrapf(err, "cannot add replicaset to metadata")
		}

		msg := &pb.StartBackup{
			BackupType:      opts.GetBackupType(),
			DbBackupName:    dbBackupName,
			OplogBackupName: oplogBackupName,
			CompressionType: opts.GetCompressionType(),
			Cypher:          opts.GetCypher(),
			OplogStartTime:  opts.GetOplogStartTime(),
			Description:     opts.Description,
			StorageName:     opts.GetStorageName(),
			MongodbVersion:  mongoDBVersion,
		}
		if err := client.startBackup(msg); err != nil {
			s.setBackupRunning(false)
			s.setOplogBackupRunning(false)
			return errors.Wrapf(err, "cannot start backup for client %s", client.ID)
		}
	}

	return nil
}

func (s *MessagesServer) getMongoDBVersion() (string, error) {
	for _, client := range s.clients {
		if client.NodeType != pb.NodeType_NODE_TYPE_MONGOS {
			return client.GetMongoDBVersion()
		}
	}
	return "", fmt.Errorf("cannot get MongoDB version. There are no agents connected to a mongod instance")
}

func (s *MessagesServer) getBalancerStatus() error {
	c := s.getClientByType(pb.NodeType_NODE_TYPE_MONGOD_CONFIGSVR)
	bs, err := c.getBalancerStatus()

	// Update the status even if there is a nil in the response.
	// If for some reason the mongoS instace has gone, we should clear s.backupStatus.balancerStatus so
	// the balancer is not re-enabled based on an old value
	s.backupStatusLock.Lock()
	s.backupStatus.balancerStatus = bs
	s.backupStatusLock.Unlock()

	if err != nil {
		return errors.Wrap(err, "cannot get balancer status. internal status was resetted")
	}
	return nil
}

// StartBalancer restarts the balancer if this is a sharded system
func (s *MessagesServer) StartBalancer() error {
	var c *Client
	s.clientsLock.Lock()
	for _, client := range s.clients {
		s.logger.Info("client node: " + client.NodeName)
		if client.NodeType == pb.NodeType_NODE_TYPE_MONGOD_CONFIGSVR {
			c = client
			break
		}
	}
	s.clientsLock.Unlock()

	if c == nil {
		return nil // This is not a sharded system. There is nothing to do.
	}

	s.logger.Info("Start client balancer")
	if err := s.triggerEvent(BeforeBalancerStartEvent, time.Now()); err != nil {
		s.logger.Errorf("cannot triggerBeforeBalancerStartEvent: %s", err)
	}

	if err := c.startBalancer(); err != nil {
		s.logger.Errorf("cannot start the balancer via client %q: %s", c.ID, err)
		return errors.Wrapf(err, "cannot start the balancer via client %q", c.ID)
	}

	s.logger.Debug("Balancer started")
	if err := s.triggerEvent(AfterBalancerStartEvent, time.Now()); err != nil {
		s.logger.Errorf("cannot trigger AfterBalancerStartEvent: %s", err)
	}

	return nil
}

func (s *MessagesServer) Stop() {
	close(s.stopChan)
}

// StopBalancer stops the balancer if this is a sharded system
func (s *MessagesServer) StopBalancer() error {
	var c *Client

	s.clientsLock.Lock()
	for _, client := range s.clients {
		if client.NodeType == pb.NodeType_NODE_TYPE_MONGOD_CONFIGSVR {
			c = client
			break
		}
	}
	s.clientsLock.Unlock()

	if c == nil {
		return nil // This is not a sharded system. There is nothing to do.
	}

	s.logger.Debug("Stopping the balancer")
	if err := s.triggerEvent(BeforeBalancerStopEvent, time.Now()); err != nil {
		s.logger.Errorf("cannot trigger BeforeBalancerStopEvent: %s", err)
	}

	if err := c.stopBalancer(); err != nil {
		return errors.Wrapf(err, "cannot stop the balancer via the %q client", c.ID)
	}

	s.logger.Debug("Balancer stopped")
	if err := s.triggerEvent(AfterBalancerStopEvent, time.Now()); err != nil {
		s.logger.Errorf("cannot trigger AfterBalancerStopEvent: %s", err)
	}

	return nil
}

// StopOplogTail calls every agent StopOplogTail(ts) method using the last oplog timestamp reported by the clients
// when they call DBBackupFinished after mongodump finish on each client. That way s.lastOplogTs has the last
// timestamp of the slowest backup
func (s *MessagesServer) StopOplogTail() error {
	if !s.isOplogBackupRunning() {
		return fmt.Errorf("backup is not running")
	}
	// This should never happen. We get the last oplog timestamp when agents call DBBackupFinished
	if s.backupStatus.lastBackupMetadata.LastOplogTs() == 0 {
		s.backupStatus.lastBackupMetadata.SetLastOplogTs(time.Now().Unix())
		s.logger.Errorf("Trying to stop the oplog tailer but last oplog timestamp is 0. Using current timestamp")
	}
	s.logger.Infof("StopOplogTs: %d (%v)",
		s.backupStatus.lastBackupMetadata.LastOplogTs(),
		time.Unix(s.backupStatus.lastBackupMetadata.LastOplogTs(), 0).Format(time.RFC3339),
	)

	var gErr error
	s.clientsLock.Lock()
	for _, client := range s.clients {
		s.logger.Debugf("Checking if client %s is running the oplog backup: %v",
			client.NodeName,
			client.isOplogTailerRunning(),
		)
		if client.isOplogTailerRunning() {
			s.logger.Debugf("Stopping oplog tail in client %s at %s",
				client.NodeName,
				time.Unix(s.backupStatus.lastBackupMetadata.LastOplogTs(), 0).Format(time.RFC3339),
			)
			err := client.stopOplogTail(s.backupStatus.lastBackupMetadata.LastOplogTs())
			if err != nil {
				gErr = errors.Wrapf(gErr, "client: %s, error: %s", client.NodeName, err)
			}
		}
	}
	s.clientsLock.Unlock()
	s.setBackupRunning(false)

	if gErr != nil {
		return errors.Wrap(gErr, "cannot stop oplog tailer")
	}
	return nil
}

func (s *MessagesServer) AddError(err error) {
	s.backupStatusLock.Lock()
	defer s.backupStatusLock.Unlock()
	s.backupStatus.lastBackupErrors = multierror.Append(s.backupStatus.lastBackupErrors, err)
}

func (s *MessagesServer) lastBackupErrors() error {
	s.backupStatusLock.Lock()
	defer s.backupStatusLock.Unlock()
	return s.backupStatus.lastBackupErrors
}

func (s *MessagesServer) WaitBackupFinish() error {
	replicasets := s.ReplicasetsRunningDBBackup()
	if len(replicasets) == 0 {
		return nil
	}
	if _, err := s.WaitForEvent(BackupFinishedEvent, WaitForever); err != nil {
		s.AddError(errors.Wrap(err, "cannot wait for backup to finish"))
	}
	return s.lastBackupErrors()
}

func (s *MessagesServer) WaitOplogBackupFinish() error {
	replicasets := s.ReplicasetsRunningOplogBackup()
	if len(replicasets) == 0 {
		return nil
	}
	<-s.oplogBackupFinishChan
	return s.lastBackupErrors()
}

func (s *MessagesServer) WaitRestoreFinish() error {
	replicasets := s.ReplicasetsRunningRestore()
	if len(replicasets) == 0 {
		return nil
	}
	<-s.restoreFinishChan
	return s.lastBackupErrors()
}

// WriteServerBackupMetadata writes the backup metadata into the coordinator's working dir
func (s *MessagesServer) WriteServerBackupMetadata(filename string) error {
	return s.backupStatus.lastBackupMetadata.WriteMetadataToFile(filepath.Join(s.workDir, filename))
}

// WriteBackupMetadata writes the metadata along with the backed up files in the destination storage
func (s *MessagesServer) WriteBackupMetadata() error {
	var c *Client

	buf, err := s.backupStatus.lastBackupMetadata.JSON()
	if err != nil {
		return errors.Wrap(err, "cannot encode last backup metadata as JSON")
	}
	// Take the first client from the connected clients list.
	// All clients should have access to all storages so any client will be able to write the metadata
	s.clientsLock.Lock()
	for _, client := range s.clients {
		if client.NodeType == pb.NodeType_NODE_TYPE_MONGOD_CONFIGSVR {
			c = client
			break
		}
	}
	s.clientsLock.Unlock()

	// Try to write the metadata to theconfig server
	if c != nil {
		return c.writeBackupMetadata(
			s.backupStatus.lastBackupMetadata.NamePrefix()+".json",
			s.backupStatus.lastBackupMetadata.metadata.StorageName,
			buf,
		)
	}

	// if there are no config servers, try to write the metadata using one of the agents that ran
	// the last backup
	for _, c := range s.backupStatus.sources {
		return c.writeBackupMetadata(
			s.backupStatus.lastBackupMetadata.NamePrefix()+".json",
			s.backupStatus.lastBackupMetadata.metadata.StorageName,
			buf,
		)
	}

	return nil
}

// WorkDir returns the server working directory.
func (s *MessagesServer) WorkDir() string {
	return s.workDir
}

// ---------------------------------------------------------------------------------------------------------------------
//                                                              gRPC methods
// ---------------------------------------------------------------------------------------------------------------------

// DBBackupFinished process backup finished message from clients.
// After the mongodump call finishes, clients should call this method to inform the event to the server
// Unless the incoming Client ID is invalid, we shouldn't return an error to the clients.
// If there is an error while processing the incoming message, it is something that should be handled on the server side
// so, the client shouldn't receive an error.
func (s *MessagesServer) DBBackupFinished(ctx context.Context, msg *pb.DBBackupFinishStatus) (
	*pb.DBBackupFinishedAck, error) {

	client := s.getClientByID(msg.GetClientId())
	if client == nil {
		return nil, fmt.Errorf("unknown client ID: %s", msg.GetClientId())
	}
	client.setDBBackupRunning(false)

	if !msg.GetOk() {
		if strings.TrimSpace(msg.GetError()) != "" {
			s.AddError(errors.New(msg.GetError()))
		}
	}

	// Most probably, we are running the backup from a secondary, but we need the last oplog timestamp
	// from the primary in order to have a consistent backup.
	primaryClient, err := s.getPrimaryClient(client.ReplicasetName)
	if err != nil {
		s.AddError(errors.Wrap(err, "cannot get the primary client"))
	}
	lastOplogTs, err := primaryClient.getPrimaryLastOplogTs()
	if err != nil {
		s.AddError(errors.Wrap(err, "cannot get primary's last oplog timestamp"))
	}
	// Keep the last (bigger) oplog timestamp from all clients running the backup.
	// When all clients finish the backup, we will call CloseAt(s.lastOplogTs) on all clients to have a consistent
	// stop time for all oplogs.
	if lastOplogTs > s.backupStatus.lastBackupMetadata.LastOplogTs() {
		s.backupStatus.lastBackupMetadata.SetLastOplogTs(lastOplogTs)
	}

	replicasets := s.ReplicasetsRunningDBBackup()
	if len(replicasets) == 0 {
		if err := s.triggerEvent(BackupFinishedEvent, time.Now()); err != nil {
			s.AddError(errors.Wrap(err, "cannot trigger BackupFinished event"))
		}
	}

	return &pb.DBBackupFinishedAck{}, nil
}

func (s *MessagesServer) Logging(stream pb.Messages_LoggingServer) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}

		level := logrus.Level(msg.GetLevel())
		msgText := strings.TrimSpace(msg.GetMessage())

		logLine := fmt.Sprintf("-> Client: %s, %s", msg.GetClientId(), msgText)
		switch level {
		case logrus.PanicLevel:
			s.logger.Panic(logLine)
		case logrus.FatalLevel:
			s.logger.Fatal(logLine)
		case logrus.ErrorLevel:
			s.logger.Error(logLine)
		case logrus.WarnLevel:
			s.logger.Warn(logLine)
		case logrus.InfoLevel:
			s.logger.Info(logLine)
		case logrus.DebugLevel:
			s.logger.Debug(logLine)
		}
	}
}

// MessagesChat is the method exposed by gRPC to stream messages between the server and agents
func (s *MessagesServer) MessagesChat(stream pb.Messages_MessagesChatServer) error {
	// This first message should be a RegisterMsg
	msg, err := stream.Recv()
	if err != nil {
		return err
	}

	clientID := msg.GetClientId()
	s.logger.Debugf("Registering new client: %s", clientID)
	if err := s.registerClient(stream, msg); err != nil {
		s.logger.Errorf("Cannot register client: %s", err)
		r := &pb.ServerMessage{
			Payload: &pb.ServerMessage_ErrorMsg{
				ErrorMsg: &pb.Error{
					Code:    pb.ErrorType_ERROR_TYPE_CLIENT_ALREADY_REGISTERED,
					Message: "",
				},
			},
		}
		err = stream.Send(r)
		if err != nil {
			s.logger.Errorf("Cannot send to client stream %s: %v", clientID, err)
		}

		return fmt.Errorf("client already exists")
	}

	r := &pb.ServerMessage{
		Payload: &pb.ServerMessage_AckMsg{AckMsg: &pb.Ack{}},
	}

	if err := stream.Send(r); err != nil {
		return err
	}

	// Keep the stream open
	<-stream.Context().Done()
	if err := s.unregisterClient(clientID); err != nil {
		s.logger.Errorf("Client %s stream was closed but cannot unregister client: %s", clientID, err)
	}

	return nil
}

func (s *MessagesServer) streamIOChans(stream pb.Messages_MessagesChatServer) (
	chan *pb.ClientMessage, chan *pb.ServerMessage) {
	in := make(chan *pb.ClientMessage, 100)
	out := make(chan *pb.ServerMessage, 100)

	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				// s.logger.Errorf("Error receiving data from the gRPC stream: %v", err)
				return
			}
			in <- msg
		}
	}()

	go func() {
		for {
			msg := <-out
			if err := stream.Send(msg); err != nil {
				//s.logger.Errorf("Cannot send message (type %T) throgh the gRPC stream: %s", msg.Payload, err)
				return
			}
		}
	}()

	return in, out
}

// OplogBackupFinished process oplog tailer finished message from clients.
// After the the oplog tailer has been closed on clients, clients should call this method to inform
// the event to the server
func (s *MessagesServer) OplogBackupFinished(ctx context.Context, msg *pb.OplogBackupFinishStatus) (
	*pb.OplogBackupFinishedAck, error) {
	client := s.getClientByID(msg.GetClientId())
	if client == nil {
		return nil, fmt.Errorf("unknown client ID: %s", msg.GetClientId())
	}
	client.setOplogTailerRunning(false)

	replicasets := s.ReplicasetsRunningOplogBackup()
	if len(replicasets) == 0 {
		if err := notify.Post(EventOplogFinish, msg.GetClientId()); err != nil {
			return nil, errors.Wrapf(err, "cannot notify OplogBackupFinished for client %s", client.ID)
		}
	}
	return &pb.OplogBackupFinishedAck{}, nil
}

// RestoreCompleted handles a replicaset restore completed messages from clients.
// After restore is completed or upon errors, each client running the restore will cann this gRPC method
// to inform the server about the restore status.
func (s *MessagesServer) RestoreCompleted(ctx context.Context, msg *pb.RestoreComplete) (
	*pb.RestoreCompletedAck, error) {
	s.logger.Debugf("Received restore completed message from client %q", msg.GetClientId())

	client := s.getClientByID(msg.GetClientId())
	if client == nil {
		s.setRestoreRunning(false)
		err := fmt.Errorf("unknown client ID: %s", msg.GetClientId())
		s.logger.Error(err)
		return nil, err
	}

	client.setRestoreRunning(false)
	if msg.GetErr() != nil && msg.GetErr().GetMessage() != "" {
		s.AddError(fmt.Errorf("received error in RestoreCompleted from client %s: %s",
			msg.GetErr().GetMessage(),
			msg.GetClientId()),
		)
	}
	replicasets := s.ReplicasetsRunningRestore()
	s.logger.Debugf("Replicasets still running the restore: %d", len(replicasets))
	if len(replicasets) == 0 {
		s.setRestoreRunning(false)
		s.logger.Debug("Sending EventRestoreFinish notification")
		if err := notify.Post(EventRestoreFinish, time.Now()); err != nil {
			err := errors.Wrapf(err, "cannot notify RestoreCompleted for client %s", client.ID)
			return nil, err
		}
	}

	s.logger.Debug("RestoreCompleted finished")
	return &pb.RestoreCompletedAck{}, nil
}

// ---------------------------------------------------------------------------------------------------------------------
//                                                        Internal helpers
// ---------------------------------------------------------------------------------------------------------------------

// func (s *MessagesServer) cancelBackup() error {
// 	if !s.isOplogBackupRunning() {
// 		return fmt.Errorf("backup is not running")
// 	}
// 	s.lock.Lock()
// 	defer s.lock.Unlock()
// 	var gerr error
// 	for _, client := range s.clients {
// 		if err := client.stopBackup(); err != nil {
// 			gerr = errors.Wrapf(err, "cannot stop backup on %s", client.ID)
// 		}
// 	}
// 	return gerr
// }

func (s *MessagesServer) getClientByID(id string) *Client {
	s.clientsLock.Lock()
	defer s.clientsLock.Unlock()

	for _, client := range s.clients {
		if client.ID == id {
			return client
		}
	}
	return nil
}

func (s *MessagesServer) getPrimaryClient(replName string) (*Client, error) {
	for _, client := range s.clients {
		if client.ReplicasetName == replName && client.isPrimary {
			return client, nil
		}
	}
	return nil, fmt.Errorf("cannot find a primary in the clients list for replicaset %s", replName)
}

func (s *MessagesServer) getClientByNodeName(name string) *Client {
	for _, client := range s.clients {
		if client.NodeName == name {
			return client
		}
	}
	return nil
}

// AllServersCmdLineOpts returns CmdLineOpts from all servers.
// This info will be saved with the backup metadata as extra information that might be
// needed to rebuild the servers
func (s *MessagesServer) AllServersCmdLineOpts() ([]*pb.Server, error) {
	serverOpts := make([]*pb.Server, 0)
	serverOptsChan := make(chan *pb.Server)

	var errs error

	wga := &sync.WaitGroup{}
	wga.Add(1)
	go func() {
		for opts := range serverOptsChan {
			serverOpts = append(serverOpts, opts)
		}
		wga.Done()
	}()

	wgb := &sync.WaitGroup{}
	s.clientsLock.Lock()

	for _, client := range s.clients {
		wgb.Add(1)
		c := client
		s.logger.Debugf("Getting CmdLineOpts from client: %s", c.ID)
		go func() {
			defer wgb.Done()
			msg, err := c.GetCmdLineOpts()
			if err != nil {
				errs = multierror.Append(errs, err)
				return
			}
			serverOptsChan <- &pb.Server{Id: c.ID, CmdLineOpts: string(msg.GetOptions())}
		}()
	}
	s.clientsLock.Unlock()
	wgb.Wait()
	close(serverOptsChan)
	wga.Wait()

	if errs != nil {
		return nil, errs
	}

	return serverOpts, nil
}

// validateReplicasetAgents will run getShardMap and parse the results on a config server.
// With that list, we can validate if we have at least one agent connected to each replicaset.
// Currently it is mandatory to have one agent ON EACH cluster member.
// Maybe in the future we can relax the requirements because having one agent per replicaset might
// be enough but it needs more testing.
func (s *MessagesServer) ValidateReplicasetAgents() error {
	var err error
	var repls []string

	s.clientsLock.Lock()

	for _, client := range s.clients {
		if client.NodeType == pb.NodeType_NODE_TYPE_MONGOD_CONFIGSVR {
			repls, err = client.listReplicasets()
			if err != nil {
				return errors.Wrap(err, "cannot get repliscasets list using getShardMap")
			}
			break
		}
	}
	s.clientsLock.Unlock()

	for _, repl := range repls {
		haveRepl := false
		for _, client := range s.clients {
			if client.ReplicasetName == repl {
				haveRepl = true
				break
			}
		}
		if !haveRepl {
			return fmt.Errorf("there are not agents for replicaset %q", repl)
		}
	}

	return nil
}

func getFileExtension(compressionType pb.CompressionType, cypher pb.Cypher) string {
	ext := ""

	switch cypher {
	case pb.Cypher_CYPHER_AES:
		ext += ".aes"
	case pb.Cypher_CYPHER_RSA:
		ext += ".rsa"
	case pb.Cypher_CYPHER_NO_CYPHER:
	default:
	}

	switch compressionType {
	case pb.CompressionType_COMPRESSION_TYPE_GZIP:
		ext += ".gz"
	case pb.CompressionType_COMPRESSION_TYPE_LZ4:
		ext += ".lz4"
	case pb.CompressionType_COMPRESSION_TYPE_SNAPPY:
		ext += ".snappy"
	}

	return ext
}

func (s *MessagesServer) isBackupRunning() bool {
	s.clientsLock.Lock()
	defer s.clientsLock.Unlock()
	return s.backupStatus.backupRunning
}

func (s *MessagesServer) isOplogBackupRunning() bool {
	s.clientsLock.Lock()
	defer s.clientsLock.Unlock()
	return s.backupStatus.oplogBackupRunning
}

func (s *MessagesServer) isRestoreRunning() bool {
	s.clientsLock.Lock()
	defer s.clientsLock.Unlock()
	return s.restoreRunning
}

func (s *MessagesServer) registerClient(stream pb.Messages_MessagesChatServer, msg *pb.ClientMessage) error {

	if msg.ClientId == "" {
		return fmt.Errorf("invalid client ID (empty)")
	}

	s.clientsLock.Lock()
	if client, exists := s.clients[msg.ClientId]; exists {
		if err := client.ping(); err != nil {
			delete(s.clients, msg.ClientId)
		} else {
			s.clientsLock.Unlock()
			return fmt.Errorf("client already exists")
		}
	}
	s.clientsLock.Unlock()

	regMsg := msg.GetRegisterMsg()
	if regMsg == nil || regMsg.NodeType == pb.NodeType_NODE_TYPE_INVALID {
		return fmt.Errorf("node type in register payload cannot be empty")
	}

	in, out := s.streamIOChans(stream)
	client := newClient(msg.ClientId, regMsg, in, out, s.logger)
	s.clients[msg.ClientId] = client

	return nil
}

func (s *MessagesServer) reset() {
	s.backupStatusLock.Lock()
	defer s.backupStatusLock.Unlock()
	s.backupStatus.lastBackupMetadata.SetLastOplogTs(0)
	s.backupStatus.backupRunning = false
	s.backupStatus.oplogBackupRunning = false
	s.restoreRunning = false
	s.backupStatus.lastBackupErrors = nil
}

func (s *MessagesServer) setBackupRunning(status bool) {
	s.clientsLock.Lock()
	defer s.clientsLock.Unlock()
	s.backupStatus.backupRunning = status
}

func (s *MessagesServer) setOplogBackupRunning(status bool) {
	s.clientsLock.Lock()
	defer s.clientsLock.Unlock()
	s.backupStatus.oplogBackupRunning = status
}

func (s *MessagesServer) setRestoreRunning(status bool) {
	s.clientsLock.Lock()
	defer s.clientsLock.Unlock()
	s.restoreRunning = status
}

func (s *MessagesServer) unregisterClient(id string) error {
	s.logger.Infof("Unregistering client %s", id)
	s.clientsLock.Lock()
	defer s.clientsLock.Unlock()

	if _, exists := s.clients[id]; !exists {
		return fmt.Errorf("unknown client")
	}

	delete(s.clients, id)
	return nil
}

func (s *MessagesServer) triggerEvent(event int, value interface{}) error {
	s.logger.Debugf("triggering event %s after %v", eventName(event), 1)
	if !isValidEvent(event) {
		return fmt.Errorf("event %d is invalid", event)
	}
	c := s.eventsNotifier.Emit(eventName(event), value)
	select {
	case <-c:
		s.logger.Debugf("triggered event %s after %v", eventName(event), 1)
	case <-time.After(1 * time.Second):
		s.logger.Debugf("timeout triggering event %s after %v", eventName(event), 1)
		close(c)
	}
	return nil
}

// WaitForEvent will wait until the desired event gets fired.
// timeout = -1 means wait forever
func (s *MessagesServer) WaitForEvent(event int, timeout time.Duration) (interface{}, error) {
	var re interface{}
	var err error
	if !isValidEvent(event) {
		return nil, fmt.Errorf("unknown event %q", eventName(event))
	}

	var t *time.Timer
	if int64(timeout) >= 0 {
		t = time.NewTimer(timeout)
	} else {
		t = time.NewTimer(0)
		t.Stop()
	}
	c := s.eventsNotifier.Once(eventName(event), func(ev *emitter.Event) {
		re = ev
	})
	select {
	case <-c:
		t.Stop()
	case <-t.C:
		err = fmt.Errorf("timeout (%v) for event %q", timeout, eventName(event))
	}

	return re, err
}

func (s *MessagesServer) BalancerStatus() *pb.BalancerStatus {
	s.backupStatusLock.Lock()
	defer s.backupStatusLock.Unlock()
	return s.backupStatus.balancerStatus
}

func isValidEvent(e int) bool {
	validEvents := []int{BackupFinishedEvent, BackupStartedEvent, BeforeBalancerStartEvent,
		AfterBalancerStartEvent, BeforeBalancerStopEvent, AfterBalancerStopEvent}

	for _, event := range validEvents {
		if event == e {
			return true
		}
	}
	return false
}

func eventName(event int) string {
	names := []string{
		"BackupFinishedEvent",
		"BackupStartedEvent",
		"BeforeBalancerStartEvent",
		"AfterBalancerStartEvent",
		"BeforeBalancerStopEvent",
		"AfterBalancerStopEvent",
	}
	if event < 0 || event >= len(names) {
		return fmt.Sprintf("invalid event %d", event)
	}
	return names[event]
}

func (s *MessagesServer) getClientByType(pb.NodeType) *Client {
	var c *Client
	s.clientsLock.Lock()
	for _, client := range s.clients {
		if client.NodeType == pb.NodeType_NODE_TYPE_MONGOD_CONFIGSVR {
			c = client
			break
		}
	}
	s.clientsLock.Unlock()
	return c
}
