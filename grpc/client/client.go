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
	"github.com/percona/mongodb-backup/internal/cluster"
	"github.com/percona/mongodb-backup/internal/dumper"
	"github.com/percona/mongodb-backup/internal/oplog"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type flusher interface {
	Flush() error
}

type Client struct {
	clientID       string
	replicasetName string
	replicasetID   string
	nodeName       string
	oplogTailer    *oplog.OplogTail
	mdbSession     *mgo.Session
	grpcClient     pb.MessagesClient
	connOpts       ConnectionOptions
	sslOpts        SSLOptions
	//
	lock       *sync.Mutex
	status     pb.Status
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

func NewClient(ctx context.Context, mdbConnOpts ConnectionOptions, mdbSSLOpts SSLOptions, conn *grpc.ClientConn) (*Client, error) {
	di := &mgo.DialInfo{
		Addrs:          []string{mdbConnOpts.Host + ":" + mdbConnOpts.Port},
		Username:       mdbConnOpts.User,
		Password:       mdbConnOpts.Password,
		AppName:        "percona-mongodb-backup",
		ReplicaSetName: mdbConnOpts.ReplicasetName,
		// ReadPreference *ReadPreference
		// Safe Safe
		// FailFast bool
		Direct: true,
	}
	mdbSession, err := mgo.DialWithInfo(di)
	mdbSession.SetMode(mgo.Eventual, true)

	replset, err := cluster.NewReplset(mdbSession)
	if err != nil {
		return nil, err
	}

	clusterIDString := ""
	if clusterID, _ := cluster.GetClusterID(mdbSession); clusterID != nil {
		clusterIDString = clusterID.Hex()
	}

	nodeType, nodeName, err := getNodeTypeAndName(mdbSession)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot get node type")
	}

	grpcClient := pb.NewMessagesClient(conn)

	stream, err := grpcClient.MessagesChat(ctx)

	log.Infof("Cluster ID     : %s", clusterIDString)
	log.Infof("Node type      : %s", nodeType)
	log.Infof("Node name      : %s", nodeName)
	log.Infof("Replicaset name: %s", replset.Name())
	log.Infof("Replicaset ID  : %s", replset.ID().Hex())

	m := &pb.ClientMessage{
		ClientID: nodeName,
		Type:     pb.ClientMessage_REGISTER,
		Payload: &pb.ClientMessage_RegisterMsg{
			RegisterMsg: &pb.Register{
				NodeType:       nodeType,
				NodeName:       nodeName,
				ClusterID:      clusterIDString,
				ReplicasetName: replset.Name(),
				ReplicasetID:   replset.ID().Hex(),
			},
		},
	}

	log.Infof("Registering node ...")
	if err := stream.Send(m); err != nil {
		return nil, errors.Wrap(err, "Failed to send registration message")
	}

	response, err := stream.Recv()
	if err != nil {
		return nil, errors.Wrap(err, "Error while receiving the registration response from the server")
	}
	if response.Type != pb.ServerMessage_REGISTRATION_OK {
		return nil, fmt.Errorf("Invalid registration response type: %d", response.Type)
	}
	log.Infof("Node registration OK.")

	c := &Client{
		clientID:       nodeName,
		replicasetName: replset.Name(),
		replicasetID:   replset.ID().Hex(),
		grpcClient:     grpcClient,
		mdbSession:     mdbSession,
		nodeName:       nodeName,
		stream:         stream,
		status: pb.Status{
			BackupType: pb.BackupType_LOGICAL,
		},
		connOpts: mdbConnOpts,
		sslOpts:  mdbSSLOpts,
		lock:     &sync.Mutex{},
		// This lock is used to sync the access to the stream Send() method.
		// For example, if the backup is running, we can receive a Ping request from
		// the server but while we are sending the Ping response, the backup can finish
		// or fail and in that case it will try to send a message to the server to inform
		// the event at the same moment we are sending the Ping response.
		// Since the access to the stream is not thread safe, we need to synchronize the
		// access to it with a mutex
		streamLock: &sync.Mutex{},
	}

	// start listening server messages
	go c.processIncommingServerMessages()

	return c, nil
}

func (c *Client) Stop() {
	c.stream.CloseSend()
}

func (c *Client) IsDbBackupRunning() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.status.DBBackUpRunning
}

func (c *Client) IsOplogBackupRunning() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.status.OplogBackupRunning
}

func (c *Client) processIncommingServerMessages() {
	for {
		msg, err := c.stream.Recv()
		if err != nil { // Stream has been closed
			return
		}

		log.Debugf("Client %s -> incoming message: %+v", c.nodeName, msg)
		switch msg.Type {
		case pb.ServerMessage_PING:
			c.processPing()
		case pb.ServerMessage_GET_BACKUP_SOURCE:
			c.processGetBackupSource()
		case pb.ServerMessage_GET_STATUS:
			c.processStatus()
		case pb.ServerMessage_START_BACKUP:
			startBackupMsg := msg.GetStartBackupMsg()
			if err := c.processStartBackup(startBackupMsg); err != nil {
				log.Printf("Cannot start backup %s", err)
				c.streamSend(&pb.ClientMessage{
					Type:     pb.ClientMessage_ERROR,
					ClientID: c.clientID,
					Payload:  &pb.ClientMessage_ErrorMsg{ErrorMsg: err.Error()},
				})
				return
			}
			c.streamSend(&pb.ClientMessage{
				Type:     pb.ClientMessage_ACK,
				ClientID: c.clientID,
				Payload:  &pb.ClientMessage_AckMsg{AckMsg: &pb.Ack{}},
			})
		case pb.ServerMessage_STOP_OPLOG_TAIL:
			stopOplogTailMsg := msg.GetStopOplogTailMsg()
			c.processStopOplogTail(stopOplogTailMsg)
		default:
			c.streamSend(&pb.ClientMessage{
				Type:     pb.ClientMessage_ERROR,
				ClientID: c.clientID,
				Payload:  &pb.ClientMessage_ErrorMsg{ErrorMsg: fmt.Sprintf("Message type %v is not implemented yet", msg.Type)},
			})
		}
	}
}

func (c *Client) processPing() {
	log.Debug("Received Ping command")
	msg := (&pb.ClientMessage{
		Type:     pb.ClientMessage_PONG,
		ClientID: c.clientID,
		Payload:  &pb.ClientMessage_PingMsg{PingMsg: &pb.Pong{Timestamp: time.Now().Unix()}},
	})
	log.Debugf("Sending PING response: %+v", msg)
	c.streamSend(msg)
}

func (c *Client) processStartBackup(msg *pb.StartBackup) error {
	log.Info("Received StartBackup command")
	log.Debugf("Received start backup command: %+v", *msg)

	c.lock.Lock()
	defer c.lock.Unlock()

	if c.status.DBBackUpRunning {
		return fmt.Errorf("Backup already running")
	}
	// Validate backup type by asking MongoDB capabilities?
	if msg.BackupType != pb.BackupType_LOGICAL {
		return fmt.Errorf("Hot Backup is not implemented yet")
	}

	fi, err := os.Stat(msg.GetDestinationDir())
	if err != nil {
		return errors.Wrapf(err, "Error while checking destination directory: %s", msg.GetDestinationDir())
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s is not a directory", msg.GetDestinationDir())
	}

	extension := c.getFileExtension(msg)

	go c.runDBBackup(msg, ".dump"+extension)
	go c.runOplogBackup(msg, ".oplog"+extension)

	return nil
}

func (c *Client) runOplogBackup(msg *pb.StartBackup, extension string) {
	log.Info("Starting oplog backup")
	writers := []io.WriteCloser{}

	switch msg.GetDestinationType() {
	case pb.DestinationType_FILE:
		fw, err := os.Create(path.Join(msg.GetDestinationDir(), msg.GetDestinationName()+extension))
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

	var err error
	c.oplogTailer, err = oplog.Open(c.mdbSession)
	if err != nil {
		log.Errorf("Cannot open the oplog tailer: %s", err)
		finishMsg := &pb.OplogBackupFinishStatus{
			ClientID: c.clientID,
			OK:       false,
			Ts:       time.Now().Unix(),
			Error:    fmt.Sprintf("Cannot open the oplog tailer: %s", err),
		}
		log.Debugf("Sending OplogFinishStatus with cannot open the tailer error to the gRPC server: %+v", *finishMsg)
		c.grpcClient.OplogBackupFinished(context.Background(), finishMsg)
		return
	}

	c.setOplogBackupRunning(true)
	n, err := io.Copy(writers[len(writers)-1], c.oplogTailer)
	if err != nil {
		c.setOplogBackupRunning(false)
		log.Errorf("Error while copying data from the oplog tailer: %s", err)
		finishMsg := &pb.OplogBackupFinishStatus{
			ClientID: c.clientID,
			OK:       false,
			Ts:       time.Now().Unix(),
			Error:    fmt.Sprintf("Cannot open the oplog tailer: %s", err),
		}
		log.Debugf("Sending OplogFinishStatus with cannot open the tailer error to the gRPC server: %+v", *finishMsg)
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
		log.Error(err)
		finishMsg := &pb.OplogBackupFinishStatus{
			ClientID: c.clientID,
			OK:       false,
			Ts:       time.Now().Unix(),
			Error:    err.Error(),
		}
		log.Debugf("Sending OplogFinishStatus with cannot open the tailer error to the gRPC server: %+v", *finishMsg)
		c.grpcClient.OplogBackupFinished(context.Background(), finishMsg)
		return
	}

	log.Info("Oplog backup completed")
	finishMsg := &pb.OplogBackupFinishStatus{
		ClientID: c.clientID,
		OK:       true,
		Ts:       time.Now().Unix(),
		Error:    "",
	}
	log.Debugf("Sending OplogFinishStatus to the gRPC server: %+v", *finishMsg)
	if ack, err := c.grpcClient.OplogBackupFinished(context.Background(), finishMsg); err != nil {
		log.Errorf("Cannot call OplogFinishStatus RPC method: %s", err)
	} else {
		log.Debugf("Received ACK from OplogFinishStatus RPC method: %+v", *ack)
	}
}

func (c *Client) runDBBackup(msg *pb.StartBackup, extension string) {
	log.Info("Starting DB backup")
	writers := []io.WriteCloser{}

	switch msg.GetDestinationType() {
	case pb.DestinationType_FILE:
		fw, err := os.Create(path.Join(msg.GetDestinationDir(), msg.GetDestinationName()+extension))
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
	log.Debugf("Calling Mongodump using: %+v", *mi)

	mdump, err := dumper.NewMongodump(mi)
	if err != nil {
		log.Fatalf("Cannot call mongodump: %s", err)
		//TODO send error
	}

	c.setDBBackupRunning(true)

	mdump.Start()
	err = mdump.Wait()

	for i := len(writers); i < 0; i-- {
		if _, ok := writers[i].(flusher); ok {
			if err = writers[i].(flusher).Flush(); err != nil {
				break
			}
		}
		if err := writers[i].Close(); err != nil {
			break
		}
	}

	c.setDBBackupRunning(false)

	if err != nil {
		log.Errorf("Cannot flush/close the MongoDump writer: %s", err)
		finishMsg := &pb.DBBackupFinishStatus{
			ClientID: c.clientID,
			OK:       false,
			Ts:       time.Now().Unix(),
			Error:    err.Error(),
		}
		c.grpcClient.DBBackupFinished(context.Background(), finishMsg)
		return
	}

	log.Info("DB dump completed")

	finishMsg := &pb.DBBackupFinishStatus{
		ClientID: c.clientID,
		OK:       true,
		Ts:       time.Now().Unix(),
		Error:    "",
	}
	log.Debugf("Sending DBBackupFinishStatus to the gRPC server: %+v", *finishMsg)
	if ack, err := c.grpcClient.DBBackupFinished(context.Background(), finishMsg); err != nil {
		log.Errorf("Cannot call DBBackupFinished RPC method: %s", err)
	} else {
		log.Debugf("Recieved ACK from DBBackupFinished RPC method: %+v", *ack)
	}
}

func (c *Client) setDBBackupRunning(status bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.status.DBBackUpRunning = status
}

func (c *Client) setOplogBackupRunning(status bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.status.OplogBackupRunning = status
}

func (c *Client) processStopOplogTail(msg *pb.StopOplogTail) {
	log.Debugf("Received StopOplogTail command for client: %s", c.clientID)
	out := &pb.ClientMessage{
		Type:     pb.ClientMessage_ACK,
		ClientID: c.clientID,
		Payload:  &pb.ClientMessage_AckMsg{AckMsg: &pb.Ack{}},
	}
	log.Debugf("Sending ACK message to the gRPC server")
	c.streamSend(out)

	c.setOplogBackupRunning(false)

	if err := c.oplogTailer.Close(); err != nil {
		log.Errorf("Cannot stop the oplog tailer: %s", err)
		finishMsg := &pb.OplogBackupFinishStatus{
			ClientID: c.clientID,
			OK:       false,
			Ts:       time.Now().Unix(),
			Error:    fmt.Sprintf("Cannot close the oplog tailer: %s", err),
		}
		log.Debugf("Sending OplogFinishStatus with error to the gRPC server: %+v", *finishMsg)
		if ack, err := c.grpcClient.OplogBackupFinished(context.Background(), finishMsg); err != nil {
			log.Errorf("Cannot call OplogBackupFinished RPC method: %s", err)
		} else {
			log.Debugf("Received ACK from OplogBackupFinished RPC method: %+v", *ack)
		}
		return
	}

	finishMsg := &pb.OplogBackupFinishStatus{
		ClientID: c.clientID,
		OK:       true,
		Ts:       time.Now().Unix(),
		Error:    "",
	}
	log.Debugf("Sending OplogFinishStatus OK to the gRPC server: %+v", *finishMsg)
	if ack, err := c.grpcClient.OplogBackupFinished(context.Background(), finishMsg); err != nil {
		log.Errorf("Cannot call OplogBackupFinished RPC method: %s", err)
	} else {
		log.Debugf("Received ACK from OplogBackupFinished RPC method: %+v", *ack)
	}
}

func (c *Client) processStatus() {
	log.Debug("Received Status command")
	c.lock.Lock()

	msg := &pb.ClientMessage{
		Type:     pb.ClientMessage_STATUS,
		ClientID: c.clientID,
		Payload: &pb.ClientMessage_StatusMsg{
			StatusMsg: &pb.Status{
				DBBackUpRunning:    c.status.DBBackUpRunning,
				OplogBackupRunning: c.status.OplogBackupRunning,
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
				OplogStartTime:     c.status.OplogStartTime,
			},
		},
	}
	c.lock.Unlock()

	log.Debugf("Sending status to the gRPC server: %+v", *msg)
	c.streamSend(msg)
}

func (c *Client) processGetBackupSource() {
	log.Debug("Received GetBackupSource command")
	r, err := cluster.NewReplset(c.mdbSession)
	if err != nil {
		log.Errorf("Cannot instantiate a cluster.NewReplset: %s", err)
		msg := &pb.ClientMessage{
			Type:     pb.ClientMessage_ERROR,
			ClientID: c.clientID,
			Payload:  &pb.ClientMessage_ErrorMsg{ErrorMsg: err.Error()},
		}
		log.Debugf("Sending error message to the RPC server: %+v", *msg)
		c.streamSend(msg)
		return
	}

	winner, err := r.BackupSource(nil)
	if err != nil {
		log.Errorf("Cannot get a backup source winner: %s", err)
		msg := &pb.ClientMessage{
			Type:     pb.ClientMessage_ERROR,
			ClientID: c.clientID,
			Payload:  &pb.ClientMessage_ErrorMsg{ErrorMsg: fmt.Sprintf("Cannot get backoup source: %s", err)},
		}
		log.Debugf("Sending error response to the RPC server: %+v", *msg)
		if err = c.streamSend(msg); err != nil {
			log.Errorf("Cannot send error response (%+v) to the RPC server: %s", msg, err)
		}
		return
	}

	msg := &pb.ClientMessage{
		Type:     pb.ClientMessage_BACKUP_SOURCE,
		ClientID: c.clientID,
		Payload:  &pb.ClientMessage_BackupSourceMsg{BackupSourceMsg: winner},
	}
	log.Debugf("Sending GetBackupSource response to the RPC server: %+v", *msg)
	c.streamSend(msg)
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
