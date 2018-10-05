package client

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/bsonfile"
	"github.com/percona/mongodb-backup/internal/cluster"
	"github.com/percona/mongodb-backup/internal/dumper"
	"github.com/percona/mongodb-backup/internal/oplog"
	"github.com/percona/mongodb-backup/internal/restore"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type flusher interface {
	Flush() error
}

type Client struct {
	id             string
	ctx            context.Context
	replicasetName string
	replicasetID   string
	nodeType       pb.NodeType
	nodeName       string
	clusterID      string
	backupDir      string
	mongoDumper    *dumper.Mongodump
	oplogTailer    *oplog.OplogTail
	mdbSession     *mgo.Session
	grpcClient     pb.MessagesClient
	connOpts       ConnectionOptions
	sslOpts        SSLOptions
	logger         *logrus.Logger
	//
	lock    *sync.Mutex
	running bool
	status  pb.Status
	//
	streamLock *sync.Mutex
	stream     pb.Messages_MessagesChatClient
}

type ConnectionOptions struct {
	Host                string
	Port                string
	User                string
	Password            string
	ReplicasetName      string
	Timeout             int
	TCPKeepAliveSeconds int
}

// Struct holding ssl-related options
type SSLOptions struct {
	UseSSL              bool
	SSLCAFile           string
	SSLPEMKeyFile       string
	SSLPEMKeyPassword   string
	SSLCRLFile          string
	SSLAllowInvalidCert bool
	SSLAllowInvalidHost bool
	SSLFipsMode         bool
}

var (
	balancerStopRetries = 3
	balancerStopTimeout = 30 * time.Second
)

func NewClient(ctx context.Context, backupDir string, mdbConnOpts ConnectionOptions, mdbSSLOpts SSLOptions,
	conn *grpc.ClientConn, logger *logrus.Logger) (*Client, error) {
	// If the provided logger is nil, create a new logger and copy Logrus standard logger level and writer
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.StandardLogger().Level)
		logger.Out = logrus.StandardLogger().Out
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
	mdbSession, err := mgo.DialWithInfo(di)

	mdbSession.SetMode(mgo.Eventual, true)

	nodeType, nodeName, err := getNodeTypeAndName(mdbSession)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot get node type")
	}

	var replicasetName, replicasetID string

	if nodeType != pb.NodeType_MONGOS {
		replset, err := cluster.NewReplset(mdbSession)
		if err != nil {
			return nil, fmt.Errorf("Cannot create a new replicaset instance: %s", err)
		}
		replicasetName = replset.Name()
		replicasetID = replset.ID().Hex()
	} else {
		nodeName = di.Addrs[0]
	}

	clusterIDString := ""
	if clusterID, _ := cluster.GetClusterID(mdbSession); clusterID != nil {
		clusterIDString = clusterID.Hex()
	}

	grpcClient := pb.NewMessagesClient(conn)

	stream, err := grpcClient.MessagesChat(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to the gRPC server")
	}

	c := &Client{
		id:             nodeName,
		ctx:            ctx,
		backupDir:      backupDir,
		replicasetName: replicasetName,
		replicasetID:   replicasetID,
		grpcClient:     grpcClient,
		mdbSession:     mdbSession,
		nodeName:       nodeName,
		nodeType:       nodeType,
		clusterID:      clusterIDString,
		stream:         stream,
		status: pb.Status{
			BackupType: pb.BackupType_LOGICAL,
		},
		connOpts: mdbConnOpts,
		sslOpts:  mdbSSLOpts,
		logger:   logger,
		lock:     &sync.Mutex{},
		running:  true,
		// This lock is used to sync the access to the stream Send() method.
		// For example, if the backup is running, we can receive a Ping request from
		// the server but while we are sending the Ping response, the backup can finish
		// or fail and in that case it will try to send a message to the server to inform
		// the event at the same moment we are sending the Ping response.
		// Since the access to the stream is not thread safe, we need to synchronize the
		// access to it with a mutex
		streamLock: &sync.Mutex{},
	}
	c.connect()
	if err := c.register(); err != nil {
		return nil, err
	}

	// start listening server messages
	go c.processIncommingServerMessages()

	return c, nil
}

func (c *Client) ID() string {
	return c.id
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
		log.Infof("Reconnecting with the gRPC server")
		stream, err := c.grpcClient.MessagesChat(c.ctx)
		if err != nil {
			time.Sleep(3 * time.Second)
			continue
		}
		c.stream = stream
		c.lock.Lock()
		c.running = true
		c.lock.Unlock()
		return
	}
}

func (c *Client) register() error {
	if !c.isRunning() {
		return fmt.Errorf("gRPC stream is closed. Cannot register the client (%s)", c.id)
	}

	m := &pb.ClientMessage{
		ClientID: c.nodeName,
		Payload: &pb.ClientMessage_RegisterMsg{
			RegisterMsg: &pb.Register{
				NodeType:       c.nodeType,
				NodeName:       c.nodeName,
				ClusterID:      c.clusterID,
				ReplicasetName: c.replicasetName,
				ReplicasetID:   c.replicasetID,
				BackupDir:      c.backupDir,
			},
		},
	}
	c.logger.Infof("Registering node ...")
	if err := c.stream.Send(m); err != nil {
		return err
	}

	response, err := c.stream.Recv()
	if err != nil {
		return err
	}
	if _, ok := response.Payload.(*pb.ServerMessage_AckMsg); !ok {
		return err
	}
	c.logger.Info("Node registered")
	return nil
}

func (c *Client) Stop() error {
	if !c.isRunning() {
		return fmt.Errorf("Client is not running")
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	c.running = false

	return c.stream.CloseSend()
}

func (c *Client) IsDBBackupRunning() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.status.RunningDBBackUp
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

		//var response *pb.ClientMessage

		c.logger.Debugf("Client %s -> incoming message: %+v", c.nodeName, msg)
		switch msg.Payload.(type) {
		case *pb.ServerMessage_GetStatusMsg:
			c.processStatus()
		case *pb.ServerMessage_BackupSourceMsg:
			c.processGetBackupSource()
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
		default:
			err = fmt.Errorf("Message type %v is not implemented yet", msg.Payload)
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
			ClientID: c.id,
			Payload:  &pb.ClientMessage_BackupSourceMsg{BackupSourceMsg: &pb.BackupSource{SourceClient: c.nodeName}},
		}
		c.logger.Debugf("Sending GetBackupSource response to the RPC server: %+v", *msg)
		c.streamSend(msg)
		return
	}

	winner, err := r.BackupSource(nil)
	if err != nil {
		c.logger.Errorf("Cannot get a backup source winner: %s", err)
		msg := &pb.ClientMessage{
			ClientID: c.id,
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
		ClientID: c.id,
		Payload:  &pb.ClientMessage_BackupSourceMsg{BackupSourceMsg: &pb.BackupSource{SourceClient: winner}},
	}
	c.logger.Debugf("%s -> Sending GetBackupSource response to the RPC server: %+v (winner: %q)", c.nodeName, *msg, winner)
	c.streamSend(msg)
}

func (c *Client) processPing() {
	c.logger.Debug("Received Ping command")
	msg := &pb.ClientMessage{
		ClientID: c.id,
		Payload:  &pb.ClientMessage_PingMsg{PingMsg: &pb.Pong{Timestamp: time.Now().Unix()}},
	}
	if err := c.streamSend(msg); err != nil {
		c.logger.Errorf("Cannot stream response to the server: %s. Out message: %+v. In message type: %T", err, *msg, msg.Payload)
	}
}

func (c *Client) processRestore(msg *pb.RestoreBackup) error {
	c.lock.Lock()
	c.status.RestoreStatus = pb.RestoreStatus_RestoringDB
	c.lock.Unlock()

	defer func() {
		c.lock.Lock()
		c.status.RestoreStatus = pb.RestoreStatus_Not_Running
		c.lock.Unlock()
	}()

	if err := c.restoreDBDump(msg); err != nil {
		err := errors.Wrap(err, "cannot restore DB backup")
		c.sendRestoreComplete(err)
		return err
	}

	c.lock.Lock()
	c.status.RestoreStatus = pb.RestoreStatus_RestoringOplog
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
		ClientID: c.id,
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
	c.logger.Infof("%s: Received StartBackup command", c.nodeName)
	c.logger.Debugf("%s: Received start backup command: %+v", c.nodeName, *msg)

	c.lock.Lock()
	defer c.lock.Unlock()

	if c.status.RunningDBBackUp {
		c.sendError(fmt.Errorf("Backup already running"))
		return
	}
	// Validate backup type by asking MongoDB capabilities?
	if msg.BackupType != pb.BackupType_LOGICAL {
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

	go c.runDBBackup(msg)
	go c.runOplogBackup(msg)

	response := &pb.ClientMessage{
		ClientID: c.id,
		Payload:  &pb.ClientMessage_AckMsg{AckMsg: &pb.Ack{}},
	}
	if err = c.streamSend(response); err != nil {
		c.logger.Errorf("processStartBackup error: cannot stream response to the server: %s. Out message: %+v. In message type: %T", err, *response, response.Payload)
	}
}

func (c *Client) processStartBalancer() (*pb.ClientMessage, error) {
	balancer, err := cluster.NewBalancer(c.mdbSession)
	if err != nil {
		return nil, errors.Wrap(err, "processStartBalancer -> cannot create a balancer instance")
	}
	if err := balancer.Start(); err != nil {
		return nil, err
	}
	c.logger.Debugf("Balancer has been started by %s", c.nodeName)

	out := &pb.ClientMessage{
		ClientID: c.id,
		Payload:  &pb.ClientMessage_AckMsg{AckMsg: &pb.Ack{}},
	}
	c.logger.Debugf("processStartBalancer Sending ACK message to the gRPC server")
	c.streamSend(out)

	return nil, nil
}

func (c *Client) processStatus() {
	c.logger.Debug("Received Status command")
	c.lock.Lock()

	msg := &pb.ClientMessage{
		ClientID: c.id,
		Payload: &pb.ClientMessage_StatusMsg{
			StatusMsg: &pb.Status{
				RunningDBBackUp:    c.status.RunningDBBackUp,
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
		ClientID: c.id,
		Payload:  &pb.ClientMessage_AckMsg{AckMsg: &pb.Ack{}},
	}
	c.logger.Debugf("processStopBalancer Sending ACK message to the gRPC server")
	c.streamSend(out)

	return nil, nil
}

func (c *Client) processStopOplogTail(msg *pb.StopOplogTail) {
	c.logger.Debugf("Received StopOplogTail command for client: %s", c.id)
	out := &pb.ClientMessage{
		ClientID: c.id,
		Payload:  &pb.ClientMessage_AckMsg{AckMsg: &pb.Ack{}},
	}
	c.logger.Debugf("Sending ACK message to the gRPC server")
	c.streamSend(out)

	c.setOplogBackupRunning(false)

	if err := c.oplogTailer.CloseAt(bson.MongoTimestamp(msg.GetTs())); err != nil {
		c.logger.Errorf("Cannot stop the oplog tailer: %s", err)
		finishMsg := &pb.OplogBackupFinishStatus{
			ClientID: c.id,
			OK:       false,
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
		ClientID: c.id,
		OK:       true,
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
	case pb.DestinationType_FILE:
		fw, err := os.Create(path.Join(c.backupDir, msg.GetDBBackupName()))
		if err != nil {
			// TODO Stream error msg to the server
		}
		writers = append(writers, fw)
	}
	switch msg.GetCypher() {
	case pb.Cypher_NO_CYPHER:
		//TODO: Add cyphers
	}

	switch msg.GetCompressionType() {
	case pb.CompressionType_GZIP:
		// chain gzip writer to the previous writer
		gzw := gzip.NewWriter(writers[len(writers)-1])
		writers = append(writers, gzw)
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
	for i := len(writers); i < 0; i-- {
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
	case pb.DestinationType_FILE:
		fw, err := os.Create(path.Join(c.backupDir, msg.GetOplogBackupName()))
		if err != nil {
			finishMsg := &pb.OplogBackupFinishStatus{
				ClientID: c.id,
				OK:       false,
				Ts:       time.Now().Unix(),
				Error:    fmt.Sprintf("Cannot create destination file: %s", err),
			}
			c.logger.Debugf("Sending OplogFinishStatus with cannot open the tailer error to the gRPC server: %+v", *finishMsg)
			c.grpcClient.OplogBackupFinished(context.Background(), finishMsg)
		}
		writers = append(writers, fw)
	}

	switch msg.GetCypher() {
	case pb.Cypher_NO_CYPHER:
		//TODO: Add cyphers
	}

	switch msg.GetCompressionType() {
	case pb.CompressionType_GZIP:
		// chain gzip writer to the previous writer
		gzw := gzip.NewWriter(writers[len(writers)-1])
		writers = append(writers, gzw)
	}

	var err error
	c.oplogTailer, err = oplog.Open(c.mdbSession)
	if err != nil {
		c.logger.Errorf("Cannot open the oplog tailer: %s", err)
		finishMsg := &pb.OplogBackupFinishStatus{
			ClientID: c.id,
			OK:       false,
			Ts:       time.Now().Unix(),
			Error:    fmt.Sprintf("Cannot open the oplog tailer: %s", err),
		}
		c.logger.Debugf("Sending OplogFinishStatus with cannot open the tailer error to the gRPC server: %+v", *finishMsg)
		c.grpcClient.OplogBackupFinished(context.Background(), finishMsg)
		return
	}

	c.setOplogBackupRunning(true)
	n, err := io.Copy(writers[len(writers)-1], c.oplogTailer)
	if err != nil {
		c.setOplogBackupRunning(false)
		c.logger.Errorf("Error while copying data from the oplog tailer: %s", err)
		finishMsg := &pb.OplogBackupFinishStatus{
			ClientID: c.id,
			OK:       false,
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

	for i := len(writers); i < 0; i-- {
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
			ClientID: c.id,
			OK:       false,
			Ts:       time.Now().Unix(),
			Error:    err.Error(),
		}
		c.logger.Debugf("Sending OplogFinishStatus with cannot open the tailer error to the gRPC server: %+v", *finishMsg)
		c.grpcClient.OplogBackupFinished(context.Background(), finishMsg)
		return
	}

	c.logger.Info("Oplog backup completed")
	finishMsg := &pb.OplogBackupFinishStatus{
		ClientID: c.id,
		OK:       true,
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
		ClientID: c.id,
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
		log.Errorf("cannot get LastWrite.OpTime.Ts from MongoDB: %s", err)
		finishMsg := &pb.DBBackupFinishStatus{
			ClientID: c.id,
			OK:       false,
			Ts:       0,
			Error:    err.Error(),
		}
		c.grpcClient.DBBackupFinished(context.Background(), finishMsg)
		return
	}

	finishMsg := &pb.DBBackupFinishStatus{
		ClientID: c.id,
		OK:       true,
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
		ClientID: c.id,
		OK:       false,
		Ts:       0,
		Error:    err.Error(),
	}
	if ack, err := c.grpcClient.DBBackupFinished(context.Background(), finishMsg); err != nil {
		c.logger.Errorf("Cannot call DBBackupFinished (cancel) RPC method: %s", err)
	} else {
		c.logger.Debugf("Recieved ACK from DBBackupFinished (cancel) RPC method: %+v", *ack)
	}
}

func (c *Client) setDBBackupRunning(status bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.status.RunningDBBackUp = status
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

func getNodeTypeAndName(session *mgo.Session) (pb.NodeType, string, error) {
	isMaster, err := cluster.NewIsMaster(session)
	if err != nil {
		return pb.NodeType_UNDEFINED, "", err
	}
	if isMaster.IsShardServer() {
		return pb.NodeType_MONGOD_SHARDSVR, isMaster.IsMasterDoc().Me, nil
	}
	// Don't change the order. A config server can also be a replica set so we need to call this BEFORE
	// calling .IsReplset()
	if isMaster.IsConfigServer() {
		return pb.NodeType_MONGOD_CONFIGSVR, isMaster.IsMasterDoc().Me, nil
	}
	if isMaster.IsReplset() {
		return pb.NodeType_MONGOD_REPLSET, isMaster.IsMasterDoc().Me, nil
	}
	if isMaster.IsMongos() {
		return pb.NodeType_MONGOS, isMaster.IsMasterDoc().Me, nil
	}
	return pb.NodeType_MONGOD, isMaster.IsMasterDoc().Me, nil
}

func (c *Client) getFileExtension(msg *pb.StartBackup) string {
	ext := ""

	switch msg.GetCypher() {
	case pb.Cypher_NO_CYPHER:
	}

	switch msg.GetCompressionType() {
	case pb.CompressionType_GZIP:
		ext = ext + ".gzip"
	}

	return ext
}

func (c *Client) restoreDBDump(opts *pb.RestoreBackup) (err error) {
	var reader io.ReadCloser
	switch opts.SourceType {
	case pb.DestinationType_FILE:
		reader, err = os.Open(path.Join(c.backupDir, opts.DBSourceName))
		if err != nil {
			return errors.Wrap(err, "cannot open restore source file")
		}
	default:
		return fmt.Errorf("Restoring from sources other than file is not implemented yet")
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
		Reader:   reader,
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

func (c *Client) restoreOplog(opts *pb.RestoreBackup) error {
	log.Infof("--> %s Restoring Oplog ...", c.id)
	log.Infof("--> %s Oplog restore completed...", c.id)

	var reader bsonfile.BSONReader
	var err error
	switch opts.SourceType {
	case pb.DestinationType_FILE:
		reader, err = bsonfile.OpenFile(path.Join(c.backupDir, opts.OplogSourceName))
		if err != nil {
			return errors.Wrap(err, "cannot open oplog restore source file")
		}
	default:
		return fmt.Errorf("Restoring oplogs from sources other than file is not implemented yet")
	}

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
	oa, err := oplog.NewOplogApply(session, reader)
	if err != nil {
		return errors.Wrap(err, "cannot instantiate the oplog applier")
	}

	if err := oa.Run(); err != nil {
		return errors.Wrap(err, "error while running the oplog applier")
	}

	return nil
}
