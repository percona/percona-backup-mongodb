package client

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/snappy"
	"github.com/google/uuid"
	"github.com/percona/mongodb-backup/bsonfile"
	"github.com/percona/mongodb-backup/internal/backup/dumper"
	"github.com/percona/mongodb-backup/internal/cluster"
	"github.com/percona/mongodb-backup/internal/oplog"
	"github.com/percona/mongodb-backup/internal/restore"
	"github.com/percona/mongodb-backup/mdbstructs"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"github.com/pierrec/lz4"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type flusher interface {
	Flush() error
}

type Client struct {
	id         string
	ctx        context.Context
	cancelFunc context.CancelFunc

	replicasetName string
	replicasetID   string
	nodeType       pb.NodeType
	nodeName       string
	clusterID      string
	backupDir      string

	mdbSession  *mgo.Session
	mgoDI       *mgo.DialInfo
	connOpts    ConnectionOptions
	sslOpts     SSLOptions
	isMasterDoc *mdbstructs.IsMaster

	mongoDumper     *dumper.Mongodump
	oplogTailer     *oplog.OplogTail
	logger          *logrus.Logger
	grpcClient      pb.MessagesClient
	dbReconnectChan chan struct{}
	//
	lock    *sync.Mutex
	running bool
	status  pb.Status
	//
	streamLock *sync.Mutex
	stream     pb.Messages_MessagesChatClient
}

type ConnectionOptions struct {
	Host                string `yaml:"host"`
	Port                string `yaml:"port"`
	User                string `yaml:"user"`
	Password            string `yaml:"password"`
	ReplicasetName      string `yaml:"replicaset_name"`
	Timeout             int    `yaml:"timeout"`
	TCPKeepAliveSeconds int    `yaml:"tcp_keep_alive_seconds"`
}

// Struct holding ssl-related options
type SSLOptions struct {
	UseSSL              bool   `yaml:"use_ssl"`
	SSLCAFile           string `yaml:"sslca_file"`
	SSLPEMKeyFile       string `yaml:"sslpem_key_file"`
	SSLPEMKeyPassword   string `yaml:"sslpem_key_password"`
	SSLCRLFile          string `yaml:"sslcrl_file"`
	SSLAllowInvalidCert bool   `yaml:"ssl_allow_invalid_cert"`
	SSLAllowInvalidHost bool   `yaml:"ssl_allow_invalid_host"`
	SSLFipsMode         bool   `yaml:"ssl_fips_mode"`
}

type shardsMap struct {
	Map map[string]string `bson:"map"`
	OK  int               `bson:"ok"`
}

var (
	balancerStopRetries = 3
	balancerStopTimeout = 30 * time.Second
	dbReconnectInterval = 30 * time.Second
	dbPingInterval      = 60 * time.Second
)

func NewClient(inctx context.Context, backupDir string, mdbConnOpts ConnectionOptions, mdbSSLOpts SSLOptions,
	conn *grpc.ClientConn, logger *logrus.Logger) (*Client, error) {
	// If the provided logger is nil, create a new logger and copy Logrus standard logger level and writer
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.StandardLogger().Level)
		logger.Out = logrus.StandardLogger().Out
	}

	grpcClient := pb.NewMessagesClient(conn)
	ctx, cancel := context.WithCancel(inctx)

	stream, err := grpcClient.MessagesChat(ctx)
	if err != nil {
		cancel()
		return nil, errors.Wrap(err, "cannot connect to the gRPC server")
	}

	di := &mgo.DialInfo{
		Addrs:          []string{mdbConnOpts.Host + ":" + mdbConnOpts.Port},
		Username:       mdbConnOpts.User,
		Password:       mdbConnOpts.Password,
		AppName:        "percona-mongodb-backup",
		ReplicaSetName: mdbConnOpts.ReplicasetName,
		// ReadPreference *ReadPreference
		// Safe Safe
		FailFast: true,
		Direct:   true,
	}

	id, err := uuid.NewRandom()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("cannot genenerate a random UUID: %s", err)
	}

	c := &Client{
		id:         id.String(),
		ctx:        ctx,
		cancelFunc: cancel,
		backupDir:  backupDir,
		grpcClient: grpcClient,
		stream:     stream,
		status: pb.Status{
			BackupType: pb.BackupType_BACKUP_TYPE_LOGICAL,
		},
		connOpts:        mdbConnOpts,
		sslOpts:         mdbSSLOpts,
		logger:          logger,
		lock:            &sync.Mutex{},
		running:         true,
		mgoDI:           di,
		dbReconnectChan: make(chan struct{}),
		// This lock is used to sync the access to the stream Send() method.
		// For example, if the backup is running, we can receive a Ping request from
		// the server but while we are sending the Ping response, the backup can finish
		// or fail and in that case it will try to send a message to the server to inform
		// the event at the same moment we are sending the Ping response.
		// Since the access to the stream is not thread safe, we need to synchronize the
		// access to it with a mutex
		streamLock: &sync.Mutex{},
	}

	if err := c.dbConnect(); err != nil {
		return nil, errors.Wrap(err, "cannot connect to the database")
	}

	if err := c.updateClientInfo(); err != nil {
		return nil, errors.Wrap(err, "cannot get MongoDB status information")
	}

	if err := c.register(); err != nil {
		return nil, err
	}

	// start listening server messages
	go c.processIncommingServerMessages()
	go c.dbWatchdog()

	return c, nil
}

func (c *Client) dbConnect() (err error) {
	c.mdbSession, err = mgo.DialWithInfo(c.mgoDI)
	if err != nil {
		return err
	}
	c.mdbSession.SetMode(mgo.Eventual, true)

	bi, err := c.mdbSession.BuildInfo()
	if err != nil {
		return errors.Wrapf(err, "Cannot get build info")
	}
	if !bi.VersionAtLeast(3, 4) {
		return fmt.Errorf("You need at least MongoDB version 3.4 to run this tool")
	}

	return nil
}

func (c *Client) updateClientInfo() (err error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	isMaster, err := cluster.NewIsMaster(c.mdbSession)
	if err != nil {
		return errors.Wrap(err, "cannot update client info")
	}

	c.isMasterDoc = isMaster.IsMasterDoc()
	c.nodeType = getNodeType(isMaster)
	c.nodeName = isMaster.IsMasterDoc().Me

	if c.nodeType != pb.NodeType_NODE_TYPE_MONGOS {
		replset, err := cluster.NewReplset(c.mdbSession)
		if err != nil {
			return fmt.Errorf("Cannot create a new replicaset instance: %s", err)
		}
		c.replicasetName = replset.Name()
		c.replicasetID = replset.ID().Hex()
	} else {
		c.nodeName = c.mgoDI.Addrs[0]
	}

	if clusterID, _ := cluster.GetClusterID(c.mdbSession); clusterID != nil {
		c.clusterID = clusterID.Hex()
	}

	return nil
}

func (c *Client) ID() string {
	return c.id
}

func (c *Client) NodeName() string {
	return c.nodeName
}

func (c *Client) ReplicasetName() string {
	return c.replicasetName
}

func (c *Client) isRunning() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.running
}

func (c *Client) connect() {
	if !c.isRunning() {
		return
	}

	for {
		c.logger.Infof("Reconnecting with the gRPC server")
		stream, err := c.grpcClient.MessagesChat(c.ctx)
		if err != nil {
			time.Sleep(3 * time.Second)
			continue
		}
		c.stream = stream
		c.lock.Lock()
		c.running = true
		c.lock.Unlock()

		return // remember we are in a reconnect for loop and we need to exit it
	}
}

func (c *Client) register() error {
	if !c.isRunning() {
		return fmt.Errorf("gRPC stream is closed. Cannot register the client (%s)", c.id)
	}

	isMaster, err := cluster.NewIsMaster(c.mdbSession)
	if err != nil {
		return errors.Wrap(err, "cannot get IsMasterDoc for register method")
	}

	m := &pb.ClientMessage{
		ClientId: c.id,
		Payload: &pb.ClientMessage_RegisterMsg{
			RegisterMsg: &pb.Register{
				NodeType:       c.nodeType,
				NodeName:       c.nodeName,
				ClusterId:      c.clusterID,
				ReplicasetName: c.replicasetName,
				ReplicasetId:   c.replicasetID,
				BackupDir:      c.backupDir,
				IsPrimary:      isMaster.IsMasterDoc().IsMaster && isMaster.IsMasterDoc().SetName != "" && c.isMasterDoc.Msg != "isdbgrid",
				IsSecondary:    isMaster.IsMasterDoc().Secondary,
			},
		},
	}
	c.logger.Infof("Registering node ...")
	if err := c.stream.Send(m); err != nil {
		return errors.Wrapf(err, "cannot register node %s", c.nodeName)
	}

	response, err := c.stream.Recv()
	if err != nil {
		return errors.Wrap(err, "error detected while receiving the RegisterMsg ACK")
	}

	switch response.Payload.(type) {
	case *pb.ServerMessage_AckMsg:
		c.logger.Info("Node registered")
		return nil
	case *pb.ServerMessage_ErrorMsg:
		return fmt.Errorf(response.GetErrorMsg().GetMessage())
	}

	return fmt.Errorf("Unknow response type %T", response.Payload)
}

func (c *Client) Stop() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.running = false

	c.cancelFunc()
	return c.stream.CloseSend()
}

func (c *Client) IsDBBackupRunning() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.status.RunningDbBackup
}

func (c *Client) IsOplogBackupRunning() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.status.RunningOplogBackup
}

func (c *Client) processIncommingServerMessages() {
	for {
		msg, err := c.stream.Recv()
		if err != nil { // Stream has been closed
			c.connect()
			c.register()
			continue
		}

		c.logger.Debugf("Incoming message: %+v", msg)
		switch msg.Payload.(type) {
		case *pb.ServerMessage_GetStatusMsg:
			c.processStatus()
		case *pb.ServerMessage_BackupSourceMsg:
			c.processGetBackupSource()
		case *pb.ServerMessage_ListReplicasets:
			c.processListReplicasets()
		case *pb.ServerMessage_PingMsg:
			c.processPing()
		//
		case *pb.ServerMessage_StartBackupMsg:
			startBackupMsg := msg.GetStartBackupMsg()
			c.processStartBackup(startBackupMsg)
		case *pb.ServerMessage_StopOplogTailMsg:
			stopOplogTailMsg := msg.GetStopOplogTailMsg()
			c.processStopOplogTail(stopOplogTailMsg)
		case *pb.ServerMessage_CancelBackupMsg:
			err = c.processCancelBackup()
			//
		case *pb.ServerMessage_RestoreBackupMsg:
			c.processRestore(msg.GetRestoreBackupMsg())
		//
		case *pb.ServerMessage_StopBalancerMsg:
			c.processStopBalancer()
		case *pb.ServerMessage_StartBalancerMsg:
			c.processStartBalancer()
		case *pb.ServerMessage_LastOplogTs:
			c.processLastOplogTs()
		default:
			err = fmt.Errorf("Client: %s, Message type %v is not implemented yet", c.NodeName(), msg.Payload)
			log.Error(err.Error())
			msg := &pb.ClientMessage{
				ClientId: c.id,
				Payload:  &pb.ClientMessage_ErrorMsg{ErrorMsg: &pb.Error{Message: err.Error()}},
			}
			c.logger.Debugf("Sending error response to the RPC server: %+v", *msg)
			if err = c.streamSend(msg); err != nil {
				c.logger.Errorf("Cannot send error response (%+v) to the RPC server: %s", msg, err)
			}
		}
	}
}

func (c *Client) dbWatchdog() {
	for {
		select {
		case <-time.After(dbPingInterval):
			if err := c.mdbSession.Ping(); err != nil {
				c.dbReconnect()
			}
		case <-c.dbReconnectChan:
			c.dbReconnect()
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Client) dbReconnect() {
	for {
		if err := c.dbConnect(); err == nil {
			return
		}
		select {
		case <-time.After(dbReconnectInterval):
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Client) processCancelBackup() error {
	err := c.mongoDumper.Stop()
	c.oplogTailer.Cancel()
	return err
}

func (c *Client) processGetBackupSource() {
	c.logger.Debug("Received GetBackupSource command")
	r, err := cluster.NewReplset(c.mdbSession)
	if err != nil {
		msg := &pb.ClientMessage{
			ClientId: c.id,
			Payload:  &pb.ClientMessage_BackupSourceMsg{BackupSourceMsg: &pb.BackupSource{SourceClient: c.nodeName}},
		}
		if err := c.streamSend(msg); err != nil {
			log.Errorf("cannot send processGetBackupSource error message: %s", err)
		}
		return
	}

	winner, err := r.BackupSource(nil)
	if err != nil {
		c.logger.Errorf("Cannot get a backup source winner: %s", err)
		msg := &pb.ClientMessage{
			ClientId: c.id,
			Payload:  &pb.ClientMessage_ErrorMsg{ErrorMsg: &pb.Error{Message: fmt.Sprintf("Cannot get backoup source: %s", err)}},
		}
		c.logger.Debugf("Sending error response to the RPC server: %+v", *msg)
		if err = c.streamSend(msg); err != nil {
			c.logger.Errorf("Cannot send error response (%+v) to the RPC server: %s", msg, err)
		}
		return
	}
	if winner == "" {
		winner = c.nodeName
	}

	msg := &pb.ClientMessage{
		ClientId: c.id,
		Payload:  &pb.ClientMessage_BackupSourceMsg{BackupSourceMsg: &pb.BackupSource{SourceClient: winner}},
	}
	c.logger.Debugf("%s -> Sending GetBackupSource response to the RPC server: %+v (winner: %q)", c.nodeName, *msg, winner)
	if err := c.streamSend(msg); err != nil {
		log.Errorf("cannot send processGetBackupSource message to the server: %s", err)
	}
}

func (c *Client) processLastOplogTs() error {
	isMaster, err := cluster.NewIsMaster(c.mdbSession)
	if err != nil {
		msg := &pb.ClientMessage{
			ClientId: c.id,
			Payload:  &pb.ClientMessage_ErrorMsg{ErrorMsg: &pb.Error{Message: fmt.Sprintf("Cannot get backoup source: %s", err)}},
		}
		c.logger.Debugf("Sending error response to the RPC server: %+v", *msg)
		if err = c.streamSend(msg); err != nil {
			c.logger.Errorf("Cannot send error response (%+v) to the RPC server: %s", msg, err)
		}
		return errors.Wrap(err, "cannot update client info")
	}

	msg := &pb.ClientMessage{
		ClientId: c.id,
		Payload:  &pb.ClientMessage_LastOplogTs{LastOplogTs: &pb.LastOplogTs{LastOplogTs: int64(isMaster.LastWrite())}},
	}
	c.logger.Debugf("%s: Sending LastOplogTs(%d) to the RPC server", c.NodeName(), isMaster.LastWrite())
	if err = c.streamSend(msg); err != nil {
		c.logger.Errorf("Cannot send error response (%+v) to the RPC server: %s", msg, err)
	}
	return nil
}

func (c *Client) processListReplicasets() error {
	var sm shardsMap
	err := c.mdbSession.Run("getShardMap", &sm)
	if err != nil {
		c.logger.Errorf("Cannot getShardMap: %s", err)
		msg := &pb.ClientMessage{
			ClientId: c.id,
			Payload:  &pb.ClientMessage_ErrorMsg{ErrorMsg: &pb.Error{Message: fmt.Sprintf("Cannot getShardMap: %s", err)}},
		}

		c.logger.Debugf("Sending error response to the RPC server: %+v", *msg)
		if err = c.streamSend(msg); err != nil {
			c.logger.Errorf("Cannot send error response (%+v) to the RPC server: %s", msg, err)
		}
		return errors.Wrap(err, "processListShards: getShardMap")
	}

	replicasets := []string{}
	/* Example
	mongos> db.getSiblingDB('admin').runCommand('getShardMap')
	{
	        "map" : {
	                "config" : "localhost:19001,localhost:19002,localhost:19003",
	                "localhost:17001" : "r1/localhost:17001,localhost:17002,localhost:17003",
	                "r1" : "r1/localhost:17001,localhost:17002,localhost:17003",
	                "r1/localhost:17001,localhost:17002,localhost:17003" : "r1/localhost:17001,localhost:17002,localhost:17003",
	        },
	        "ok" : 1
	}
	*/
	for key, _ := range sm.Map {
		m := strings.Split(key, "/")
		if len(m) < 2 {
			continue
		}
		replicasets = append(replicasets, m[0])
	}

	msg := &pb.ClientMessage{
		ClientId: c.id,
		Payload:  &pb.ClientMessage_ReplicasetsMsg{ReplicasetsMsg: &pb.Replicasets{Replicasets: replicasets}},
	}
	if err := c.streamSend(msg); err != nil {
		log.Errorf("cannot send processListReplicasets message to the server: %v", err)
	}
	return nil
}

func (c *Client) processPing() {
	c.logger.Debug("Received Ping command")
	c.updateClientInfo()
	pongMsg := &pb.Pong{
		Timestamp:           time.Now().Unix(),
		NodeType:            c.nodeType,
		ReplicaSetUuid:      c.replicasetID,
		ReplicaSetVersion:   0,
		IsPrimary:           c.isMasterDoc.IsMaster && c.isMasterDoc.SetName != "" && c.isMasterDoc.Msg != "isdbgrid",
		IsSecondary:         !c.isMasterDoc.IsMaster,
		IsTailing:           c.IsOplogBackupRunning(),
		LastTailedTimestamp: c.oplogTailer.LastOplogTimestamp().Time().Unix(),
	}

	msg := &pb.ClientMessage{
		ClientId: c.id,
		Payload:  &pb.ClientMessage_PongMsg{PongMsg: pongMsg},
	}

	if err := c.streamSend(msg); err != nil {
		c.logger.Errorf("Cannot stream response to the server: %s. Out message: %+v. In message type: %T", err, *msg, msg.Payload)
	}
}

func (c *Client) processRestore(msg *pb.RestoreBackup) error {
	c.lock.Lock()
	c.status.RestoreStatus = pb.RestoreStatus_RESTORE_STATUS_RESTORINGDB
	c.lock.Unlock()

	defer func() {
		c.lock.Lock()
		c.status.RestoreStatus = pb.RestoreStatus_RESTORE_STATUS_NOT_RUNNING
		c.lock.Unlock()
	}()

	c.sendACK()

	if err := c.restoreDBDump(msg); err != nil {
		err := errors.Wrap(err, "cannot restore DB backup")
		c.sendRestoreComplete(err)
		return err
	}

	c.lock.Lock()
	c.status.RestoreStatus = pb.RestoreStatus_RESTORE_STATUS_RESTORINGOPLOG
	c.lock.Unlock()

	if err := c.restoreOplog(msg); err != nil {
		err := errors.Wrap(err, "cannot restore Oplog backup")
		if err1 := c.sendRestoreComplete(err); err1 != nil {
			err = errors.Wrapf(err, "cannot send backup complete message: %s", err1)
		}
		return err
	}

	if err := c.sendRestoreComplete(nil); err != nil {
		return errors.Wrap(err, "cannot send backup completed message")
	}
	return nil
}

func (c *Client) sendRestoreComplete(err error) error {
	msg := &pb.RestoreComplete{
		ClientId: c.id,
		Err:      &pb.Error{},
	}
	if err != nil {
		msg.Err = &pb.Error{
			Message: err.Error(),
		}
	}

	_, err = c.grpcClient.RestoreCompleted(context.Background(), msg)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) processStartBackup(msg *pb.StartBackup) {
	c.logger.Info("Received StartBackup command")

	c.lock.Lock()
	defer c.lock.Unlock()

	if c.status.RunningDbBackup {
		c.sendError(fmt.Errorf("Backup already running"))
		return
	}
	// Validate backup type by asking MongoDB capabilities?
	if msg.BackupType == pb.BackupType_BACKUP_TYPE_INVALID {
		c.sendError(fmt.Errorf("Backup type should be hot or logical"))
		return
	}
	if msg.BackupType != pb.BackupType_BACKUP_TYPE_LOGICAL {
		c.sendError(fmt.Errorf("Hot Backup is not implemented yet"))
		return
	}

	fi, err := os.Stat(c.backupDir)
	if err != nil {
		c.sendError(errors.Wrapf(err, "Error while checking destination directory: %s", c.backupDir))
		return
	}
	if !fi.IsDir() {
		c.sendError(fmt.Errorf("%s is not a directory", c.backupDir))
		return
	}
	// Send the ACK message and work on the background. When the process finishes, it will send the
	// gRPC messages to signal that the backup has been completed
	c.sendACK()
	// There is a delay when starting a new go-routine. We need to instantiate c.oplogTailer here otherwise
	// if we run go c.runOplogBackup(msg) and then WaitUntilFirstDoc(), the oplogTailer can be nill because
	// of the delay
	c.oplogTailer, err = oplog.Open(c.mdbSession)
	if err != nil {
		c.logger.Errorf("Cannot open the oplog tailer: %s", err)
		finishMsg := &pb.OplogBackupFinishStatus{
			ClientId: c.id,
			Ok:       false,
			Ts:       time.Now().Unix(),
			Error:    fmt.Sprintf("Cannot open the oplog tailer: %s", err),
		}
		c.logger.Debugf("Sending OplogFinishStatus with cannot open the tailer error to the gRPC server: %+v", *finishMsg)
		c.grpcClient.OplogBackupFinished(context.Background(), finishMsg)
		return
	}

	go c.runOplogBackup(msg)
	// Wait until we have at least one document from the tailer to start the backup only after we have
	// documents in the oplog tailer.
	c.oplogTailer.WaitUntilFirstDoc()
	go c.runDBBackup(msg)
}

func (c *Client) processStartBalancer() (*pb.ClientMessage, error) {
	balancer, err := cluster.NewBalancer(c.mdbSession)
	if err != nil {
		return nil, errors.Wrap(err, "processStartBalancer -> cannot create a balancer instance")
	}
	if err := balancer.Start(); err != nil {
		return nil, err
	}
	c.logger.Debugf("Balancer has been started by me")

	out := &pb.ClientMessage{
		ClientId: c.id,
		Payload:  &pb.ClientMessage_AckMsg{AckMsg: &pb.Ack{}},
	}
	c.logger.Debugf("processStartBalancer Sending ACK message to the gRPC server")
	if err := c.streamSend(out); err != nil {
		log.Errorf("cannot send processStartBalancer response to the server: %s", err)
	}

	return nil, nil
}

func (c *Client) processStatus() {
	c.logger.Debug("Received Status command")
	c.lock.Lock()

	msg := &pb.ClientMessage{
		ClientId: c.id,
		Payload: &pb.ClientMessage_StatusMsg{
			StatusMsg: &pb.Status{
				RunningDbBackup:    c.status.RunningDbBackup,
				RunningOplogBackup: c.status.RunningOplogBackup,
				BackupType:         c.status.BackupType,
				BytesSent:          c.status.BytesSent,
				LastOplogTs:        c.status.LastOplogTs,
				BackupCompleted:    c.status.BackupCompleted,
				LastError:          c.status.LastError,
				ReplicasetVersion:  c.status.ReplicasetVersion,
				DestinationType:    c.status.DestinationType,
				DestinationName:    c.status.DestinationName,
				DestinationDir:     c.status.DestinationDir,
				CompressionType:    c.status.CompressionType,
				Cypher:             c.status.Cypher,
				StartOplogTs:       c.status.StartOplogTs,
			},
		},
	}
	c.lock.Unlock()

	c.logger.Debugf("Sending status to the gRPC server: %+v", *msg)
	if err := c.streamSend(msg); err != nil {
		c.logger.Errorf("Cannot stream response to the server: %s. Out message: %+v. In message type: %T", err, msg, msg.Payload)
	}
}

func (c *Client) processStopBalancer() (*pb.ClientMessage, error) {
	balancer, err := cluster.NewBalancer(c.mdbSession)
	if err != nil {
		return nil, errors.Wrap(err, "processStopBalancer -> cannot create a balancer instance")
	}
	if err := balancer.StopAndWait(balancerStopRetries, balancerStopTimeout); err != nil {
		return nil, err
	}

	c.logger.Debugf("Balancer has been stopped by %s", c.nodeName)
	out := &pb.ClientMessage{
		ClientId: c.id,
		Payload:  &pb.ClientMessage_AckMsg{AckMsg: &pb.Ack{}},
	}
	c.logger.Debugf("processStopBalancer Sending ACK message to the gRPC server")
	if err := c.streamSend(out); err != nil {
		log.Errorf("cannot send processStopBalancer response to the server: %s", err)
	}

	return nil, nil
}

func (c *Client) processStopOplogTail(msg *pb.StopOplogTail) {
	c.logger.Debugf("Received StopOplogTail command for client: %s", c.id)
	out := &pb.ClientMessage{
		ClientId: c.id,
		Payload:  &pb.ClientMessage_AckMsg{AckMsg: &pb.Ack{}},
	}
	c.logger.Debugf("Sending ACK message to the gRPC server")
	if err := c.streamSend(out); err != nil {
		log.Errorf("cannot send processStopOplogTail ACK to the server: %s", err)
	}

	c.setOplogBackupRunning(false)

	if err := c.oplogTailer.CloseAt(bson.MongoTimestamp(msg.GetTs())); err != nil {
		c.logger.Errorf("Cannot stop the oplog tailer: %s", err)
		finishMsg := &pb.OplogBackupFinishStatus{
			ClientId: c.id,
			Ok:       false,
			Ts:       time.Now().Unix(),
			Error:    fmt.Sprintf("Cannot close the oplog tailer: %s", err),
		}
		c.logger.Debugf("Sending OplogFinishStatus with error to the gRPC server: %+v", *finishMsg)
		if ack, err := c.grpcClient.OplogBackupFinished(context.Background(), finishMsg); err != nil {
			c.logger.Errorf("Cannot call OplogBackupFinished RPC method: %s", err)
		} else {
			c.logger.Debugf("Received ACK from OplogBackupFinished RPC method: %+v", *ack)
		}
		return
	}

	finishMsg := &pb.OplogBackupFinishStatus{
		ClientId: c.id,
		Ok:       true,
		Ts:       time.Now().Unix(),
		Error:    "",
	}
	c.logger.Debugf("Sending OplogFinishStatus OK to the gRPC server: %+v", *finishMsg)
	if ack, err := c.grpcClient.OplogBackupFinished(context.Background(), finishMsg); err != nil {
		c.logger.Errorf("Cannot call OplogBackupFinished RPC method: %s", err)
	} else {
		c.logger.Debugf("Received ACK from OplogBackupFinished RPC method: %+v", *ack)
	}
}

func (c *Client) runDBBackup(msg *pb.StartBackup) {
	var err error
	c.logger.Info("Starting DB backup")
	writers := []io.WriteCloser{}

	switch msg.GetDestinationType() {
	case pb.DestinationType_DESTINATION_TYPE_FILE:
		fw, err := os.Create(path.Join(c.backupDir, msg.GetDbBackupName()))
		if err != nil {
			log.Errorf("Cannot create backup file: %s", err)
			// TODO Stream error msg to the server
		}
		writers = append(writers, fw)
	}

	switch msg.GetCypher() {
	case pb.Cypher_CYPHER_NO_CYPHER:
		//TODO: Add cyphers
	}

	// chain compression writer to the previous writer
	switch msg.GetCompressionType() {
	case pb.CompressionType_COMPRESSION_TYPE_GZIP:
		gzw := gzip.NewWriter(writers[len(writers)-1])
		writers = append(writers, gzw)
	case pb.CompressionType_COMPRESSION_TYPE_LZ4:
		lz4w := lz4.NewWriter(writers[len(writers)-1])
		writers = append(writers, lz4w)
	case pb.CompressionType_COMPRESSION_TYPE_SNAPPY:
		snappyw := snappy.NewWriter(writers[len(writers)-1])
		writers = append(writers, snappyw)
	}

	mi := &dumper.MongodumpInput{
		Host:     c.connOpts.Host,
		Port:     c.connOpts.Port,
		Username: c.connOpts.User,
		Password: c.connOpts.Password,
		Gzip:     false,
		Oplog:    false,
		Threads:  1,
		Writer:   writers[len(writers)-1],
	}
	c.logger.Debugf("Calling Mongodump using: %+v", *mi)

	c.mongoDumper, err = dumper.NewMongodump(mi)
	if err != nil {
		c.logger.Fatalf("Cannot call mongodump: %s", err)
		//TODO send error
	}

	c.setDBBackupRunning(true)

	c.mongoDumper.Start()
	dumpErr := c.mongoDumper.Wait()

	var wErr error
	for i := len(writers) - 1; i >= 0; i-- {
		if _, ok := writers[i].(flusher); ok {
			if err = writers[i].(flusher).Flush(); err != nil {
				wErr = errors.Wrap(err, "Cannot flush writer")
			}
		}
		if err := writers[i].Close(); err != nil {
			wErr = errors.Wrap(err, "Cannot close writer")
		}
	}
	if wErr != nil {
		c.logger.Errorf("Cannot flush/close the MongoDump writer: %s", err)
		c.sendError(err)
		return
	}

	c.setDBBackupRunning(false)

	if dumpErr != nil {
		c.sendError(fmt.Errorf("backup was cancelled"))
		c.logger.Info("DB dump cancelled")
		return
	}

	c.logger.Info("DB dump completed")
	c.sendBackupFinishOK()
}

func (c *Client) runOplogBackup(msg *pb.StartBackup) {
	c.logger.Info("Starting oplog backup")
	writers := []io.WriteCloser{}

	switch msg.GetDestinationType() {
	case pb.DestinationType_DESTINATION_TYPE_FILE:
		fw, err := os.Create(path.Join(c.backupDir, msg.GetOplogBackupName()))
		if err != nil {
			finishMsg := &pb.OplogBackupFinishStatus{
				ClientId: c.id,
				Ok:       false,
				Ts:       time.Now().Unix(),
				Error:    fmt.Sprintf("Cannot create destination file: %s", err),
			}
			c.logger.Debugf("Sending OplogFinishStatus with cannot open the tailer error to the gRPC server: %+v", *finishMsg)
			c.grpcClient.OplogBackupFinished(context.Background(), finishMsg)
		}
		writers = append(writers, fw)
	}

	switch msg.GetCypher() {
	case pb.Cypher_CYPHER_NO_CYPHER:
		//TODO: Add cyphers
	}

	switch msg.GetCompressionType() {
	case pb.CompressionType_COMPRESSION_TYPE_GZIP:
		// chain gzip writer to the previous writer
		gzw := gzip.NewWriter(writers[len(writers)-1])
		writers = append(writers, gzw)
	case pb.CompressionType_COMPRESSION_TYPE_LZ4:
		lz4w := lz4.NewWriter(writers[len(writers)-1])
		writers = append(writers, lz4w)
	case pb.CompressionType_COMPRESSION_TYPE_SNAPPY:
		snappyw := snappy.NewWriter(writers[len(writers)-1])
		writers = append(writers, snappyw)
	}

	c.setOplogBackupRunning(true)
	n, err := io.Copy(writers[len(writers)-1], c.oplogTailer)
	if err != nil {
		c.setOplogBackupRunning(false)
		c.logger.Errorf("Error while copying data from the oplog tailer: %s", err)
		finishMsg := &pb.OplogBackupFinishStatus{
			ClientId: c.id,
			Ok:       false,
			Ts:       time.Now().Unix(),
			Error:    fmt.Sprintf("Cannot open the oplog tailer: %s", err),
		}
		c.logger.Debugf("Sending OplogFinishStatus with cannot open the tailer error to the gRPC server: %+v", *finishMsg)
		c.grpcClient.OplogBackupFinished(context.Background(), finishMsg)
		return
	}

	c.lock.Lock()
	c.status.BytesSent += uint64(n)
	c.lock.Unlock()

	for i := len(writers) - 1; i >= 0; i-- {
		if _, ok := writers[i].(flusher); ok {
			if err = writers[i].(flusher).Flush(); err != nil {
				break
			}
		}
		if err = writers[i].Close(); err != nil {
			break
		}
	}
	c.setOplogBackupRunning(false)
	if err != nil {
		err := fmt.Errorf("Cannot flush/close oplog chained writer: %s", err)
		c.logger.Error(err)
		finishMsg := &pb.OplogBackupFinishStatus{
			ClientId: c.id,
			Ok:       false,
			Ts:       time.Now().Unix(),
			Error:    err.Error(),
		}
		c.logger.Debugf("Sending OplogFinishStatus with cannot open the tailer error to the gRPC server: %+v", *finishMsg)
		c.grpcClient.OplogBackupFinished(context.Background(), finishMsg)
		return
	}

	c.logger.Info("Oplog backup completed")
	finishMsg := &pb.OplogBackupFinishStatus{
		ClientId: c.id,
		Ok:       true,
		Ts:       time.Now().Unix(),
		Error:    "",
	}
	c.logger.Debugf("Sending OplogFinishStatus to the gRPC server: %+v", *finishMsg)
	if ack, err := c.grpcClient.OplogBackupFinished(context.Background(), finishMsg); err != nil {
		c.logger.Errorf("Cannot call OplogFinishStatus RPC method: %s", err)
	} else {
		c.logger.Debugf("Received ACK from OplogFinishStatus RPC method: %+v", *ack)
	}
}

func (c *Client) sendACK() {
	response := &pb.ClientMessage{
		ClientId: c.id,
		Payload:  &pb.ClientMessage_AckMsg{AckMsg: &pb.Ack{}},
	}
	if err := c.streamSend(response); err != nil {
		c.logger.Errorf("sendACK error: cannot stream response to the server: %s. Out message: %+v. In message type: %T", err, *response, response.Payload)
	}
}

func (c *Client) sendBackupFinishOK() {
	ismaster, err := cluster.NewIsMaster(c.mdbSession)
	// This should never happen.
	if err != nil {
		c.logger.Errorf("cannot get LastWrite.OpTime.Ts from MongoDB: %s", err)
		finishMsg := &pb.DBBackupFinishStatus{
			ClientId: c.id,
			Ok:       false,
			Ts:       0,
			Error:    err.Error(),
		}
		c.grpcClient.DBBackupFinished(context.Background(), finishMsg)
		return
	}

	finishMsg := &pb.DBBackupFinishStatus{
		ClientId: c.id,
		Ok:       true,
		Ts:       int64(ismaster.IsMasterDoc().LastWrite.OpTime.Ts),
		Error:    "",
	}
	c.logger.Debugf("Sending DBBackupFinishStatus to the gRPC server: %+v", *finishMsg)
	if ack, err := c.grpcClient.DBBackupFinished(context.Background(), finishMsg); err != nil {
		c.logger.Errorf("Cannot call DBBackupFinished RPC method: %s", err)
	} else {
		c.logger.Debugf("Recieved ACK from DBBackupFinished RPC method: %+v", *ack)
	}
}

func (c *Client) sendError(err error) {
	finishMsg := &pb.DBBackupFinishStatus{
		ClientId: c.id,
		Ok:       false,
		Ts:       0,
		Error:    err.Error(),
	}
	if ack, err := c.grpcClient.DBBackupFinished(context.Background(), finishMsg); err != nil {
		c.logger.Errorf("Cannot call DBBackupFinished with error (%s) RPC method: %s", finishMsg.Error, err)
	} else {
		c.logger.Debugf("Recieved ACK from DBBackupFinished with error RPC method: %+v", *ack)
	}
}

func (c *Client) setDBBackupRunning(status bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.status.RunningDbBackup = status
}

func (c *Client) setOplogBackupRunning(status bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.status.RunningOplogBackup = status
}

func (c *Client) streamSend(msg *pb.ClientMessage) error {
	c.streamLock.Lock()
	defer c.streamLock.Unlock()
	return c.stream.Send(msg)
}

func getNodeType(isMaster *cluster.IsMaster) pb.NodeType {
	if isMaster.IsShardServer() {
		return pb.NodeType_NODE_TYPE_MONGOD_SHARDSVR
	}
	// Don't change the order. A config server can also be a replica set so we need to call this BEFORE
	// calling .IsReplset()
	if isMaster.IsConfigServer() {
		return pb.NodeType_NODE_TYPE_MONGOD_CONFIGSVR
	}
	if isMaster.IsReplset() {
		return pb.NodeType_NODE_TYPE_MONGOD_REPLSET
	}
	if isMaster.IsMongos() {
		return pb.NodeType_NODE_TYPE_MONGOS
	}
	return pb.NodeType_NODE_TYPE_MONGOD
}

func (c *Client) restoreDBDump(opts *pb.RestoreBackup) (err error) {
	readers := []io.ReadCloser{}

	switch opts.SourceType {
	case pb.DestinationType_DESTINATION_TYPE_FILE:
		reader, err := os.Open(path.Join(c.backupDir, opts.DbSourceName))
		if err != nil {
			return errors.Wrap(err, "cannot open restore source file")
		}
		readers = append(readers, reader)
	default:
		return fmt.Errorf("Restoring from sources other than file is not implemented yet")
	}

	switch opts.GetCompressionType() {
	case pb.CompressionType_COMPRESSION_TYPE_GZIP:
		gzr, err := gzip.NewReader(readers[len(readers)-1])
		if err != nil {
			return errors.Wrap(err, "cannot create a gzip reader")
		}
		readers = append(readers, gzr)
	case pb.CompressionType_COMPRESSION_TYPE_LZ4:
		lz4r := lz4.NewReader(readers[len(readers)-1])
		readers = append(readers, ioutil.NopCloser(lz4r))
	case pb.CompressionType_COMPRESSION_TYPE_SNAPPY:
		snappyr := snappy.NewReader(readers[len(readers)-1])
		readers = append(readers, ioutil.NopCloser(snappyr))
	}

	// We need to set Archive = "-" so MongoRestore can use the provided reader.
	// Why we don't want to use archive? Because if we use Archive, we are limited to files in the local
	// filesystem and those files could be plain dumps or gzipped dump files but we want to be able to:
	// 1. Read from a stream (S3?)
	// 2. Use different compression algorithms.
	// 3. Read from encrypted backups.
	// That's why we are providing our own reader to MongoRestore
	input := &restore.MongoRestoreInput{
		Archive:  "-",
		DryRun:   false,
		Host:     c.connOpts.Host,
		Port:     c.connOpts.Port,
		Username: c.connOpts.User,
		Password: c.connOpts.Password,
		Gzip:     false,
		Oplog:    false,
		Threads:  1,
		Reader:   readers[len(readers)-1],
		// A real restore would be applied to a just created and empty instance and it should be
		// configured to run without user authentication.
		// For testing purposes, we can skip restoring users and roles.
		SkipUsersAndRoles: opts.SkipUsersAndRoles,
	}

	r, err := restore.NewMongoRestore(input)
	if err != nil {
		return errors.Wrap(err, "cannot instantiate mongo restore instance")
	}

	if err := r.Start(); err != nil {
		return errors.Wrap(err, "cannot start restore")
	}

	if err := r.Wait(); err != nil {
		return errors.Wrap(err, "error while trying to restore")
	}

	return nil
}

func (c *Client) restoreOplog(opts *pb.RestoreBackup) (err error) {
	// var reader bsonfile.BSONReader
	readers := []io.ReadCloser{}

	switch opts.SourceType {
	case pb.DestinationType_DESTINATION_TYPE_FILE:
		filer, err := os.Open(path.Join(c.backupDir, opts.OplogSourceName))
		if err != nil {
			return errors.Wrap(err, "cannot open oplog restore source file")
		}
		readers = append(readers, filer)
	default:
		return fmt.Errorf("Restoring oplogs from sources other than file is not implemented yet")
	}

	switch opts.GetCompressionType() {
	case pb.CompressionType_COMPRESSION_TYPE_GZIP:
		gzr, err := gzip.NewReader(readers[len(readers)-1])
		if err != nil {
			return errors.Wrap(err, "cannot create a gzip reader")
		}
		readers = append(readers, gzr)
	case pb.CompressionType_COMPRESSION_TYPE_LZ4:
		lz4r := lz4.NewReader(readers[len(readers)-1])
		readers = append(readers, ioutil.NopCloser(lz4r))
	case pb.CompressionType_COMPRESSION_TYPE_SNAPPY:
		snappyr := snappy.NewReader(readers[len(readers)-1])
		readers = append(readers, ioutil.NopCloser(snappyr))
	}

	bsonReader, err := bsonfile.NewBSONReader(readers[len(readers)-1])

	di := &mgo.DialInfo{
		Addrs:    []string{fmt.Sprintf("%s:%s", c.connOpts.Host, c.connOpts.Port)},
		Username: c.connOpts.User,
		Password: c.connOpts.Password,
	}
	session, err := mgo.DialWithInfo(di)
	if err != nil {
		return errors.Wrap(err, "cannot connect to MongoDB to apply the oplog")
	}
	// Replay the oplog
	oa, err := oplog.NewOplogApply(session, bsonReader)
	if err != nil {
		return errors.Wrap(err, "cannot instantiate the oplog applier")
	}

	if err := oa.Run(); err != nil {
		return errors.Wrap(err, "error while running the oplog applier")
	}

	return nil
}
