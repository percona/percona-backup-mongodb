package api

import (
	"context"
	"path/filepath"
	"time"

	"github.com/percona/mongodb-backup/grpc/server"
	pbapi "github.com/percona/mongodb-backup/proto/api"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"github.com/sirupsen/logrus"
)

type ApiServer struct {
	messagesServer *server.MessagesServer
}

func NewApiServer(server *server.MessagesServer) *ApiServer {
	return &ApiServer{
		messagesServer: server,
	}
}

var (
	logger = logrus.New()
)

func init() {
	logger.SetLevel(logrus.DebugLevel)
}

func (a *ApiServer) GetClients(m *pbapi.Empty, stream pbapi.Api_GetClientsServer) error {
	for _, clientsByReplicasets := range a.messagesServer.ClientsByReplicaset() {
		for _, client := range clientsByReplicasets {
			status := client.Status()

			c := &pbapi.Client{
				ID:              client.ID,
				NodeType:        client.NodeType.String(),
				NodeName:        client.NodeName,
				ClusterID:       client.ClusterID,
				ReplicasetName:  client.ReplicasetName,
				ReplicasetID:    client.ReplicasetUUID,
				LastCommandSent: client.LastCommandSent,
				LastSeen:        client.LastSeen.Unix(),
				Status: &pbapi.ClientStatus{
					ReplicaSetUUID:    client.ReplicasetUUID,
					ReplicaSetName:    client.ReplicasetName,
					ReplicaSetVersion: status.ReplicasetVersion,
					RunningDBBackup:   status.RunningDBBackUp,
					Compression:       status.CompressionType.String(),
					Encrypted:         status.Cypher.String(),
					Destination:       status.DestinationType.String(),
					Filename:          filepath.Join(status.DestinationDir, status.DestinationName),
					BackupType:        status.BackupType.String(),
					StartOplogTs:      status.StartOplogTs,
					LastOplogTs:       status.LastOplogTs,
					LastError:         status.LastError,
					Finished:          status.BackupCompleted,
				},
			}
			stream.Send(c)
		}
	}
	return nil
}

// StartBackup starts a backup by calling server's StartBackup gRPC method
// This call waits until the backup finish
func (a *ApiServer) RunBackup(ctx context.Context, opts *pbapi.RunBackupParams) (*pbapi.Error, error) {

	msg := &pb.StartBackup{
		OplogStartTime:  time.Now().Unix(),
		BackupType:      pb.BackupType(opts.BackupType),
		DestinationType: pb.DestinationType(opts.DestinationType),
		CompressionType: pb.CompressionType(opts.CompressionType),
		Cypher:          pb.Cypher(opts.Cypher),
		// DBBackupName & OplogBackupName are going to be set in server.go
		// We cannot set them here because the backup name will include the replicaset name so, it will
		// be different for each client/MongoDB instance
		// Here we are just using the same pb.StartBackup message to avoid declaring a new structure.
	}

	logger.Debug("Stopping the balancer")
	if err := a.messagesServer.StopBalancer(); err != nil {
		return &pbapi.Error{Message: err.Error()}, err
	}

	if err := a.messagesServer.StartBackup(msg); err != nil {
		return &pbapi.Error{Message: err.Error()}, err
	}
	logger.Debug("Backup started")
	logger.Debug("Waiting for backup to finish")

	a.messagesServer.WaitBackupFinish()
	logger.Debug("Stopping oplog")
	err := a.messagesServer.StopOplogTail()
	if err != nil {
		logger.Fatalf("Cannot stop oplog tailer %s", err)
		return &pbapi.Error{Message: err.Error()}, err
	}
	logger.Debug("Waiting oplog to finish")
	a.messagesServer.WaitOplogBackupFinish()
	logger.Debug("Oplog finished")

	logger.Debug("Starting the balancer")
	if err := a.messagesServer.StartBalancer(); err != nil {
		return &pbapi.Error{Message: err.Error()}, err
	}
	return &pbapi.Error{}, nil
}
