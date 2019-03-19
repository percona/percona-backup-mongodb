package api

import (
	"context"
	"os"
	"time"

	"github.com/kr/pretty"
	"github.com/percona/percona-backup-mongodb/grpc/server"
	pbapi "github.com/percona/percona-backup-mongodb/proto/api"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Apiserver has unexported fields and all the methods for API calls
type Server struct {
	messagesServer *server.MessagesServer
	logger         *logrus.Logger
}

// NewServer returns a new API Server
func NewServer(server *server.MessagesServer) *Server {
	s := &Server{
		messagesServer: server,
		logger:         logrus.New(),
	}
	if os.Getenv("DEBUG") == "1" {
		s.logger.SetLevel(logrus.DebugLevel)
	}

	return s
}

func (a *Server) GetClients(m *pbapi.Empty, stream pbapi.Api_GetClientsServer) error {
	if err := a.messagesServer.RefreshClients(); err != nil {
		return errors.Wrap(err, "cannot refresh clients list")
	}

	for _, clientsByReplicasets := range a.messagesServer.ClientsByReplicaset() {
		for _, client := range clientsByReplicasets {
			status := client.Status()

			c := &pbapi.Client{
				Id:              client.ID,
				NodeType:        client.NodeType.String(),
				NodeName:        client.NodeName,
				ClusterId:       client.ClusterID,
				ReplicasetName:  client.ReplicasetName,
				ReplicasetId:    client.ReplicasetUUID,
				LastCommandSent: client.LastCommandSent,
				LastSeen:        client.LastSeen.Unix(),
				Status: &pbapi.ClientStatus{
					ReplicasetUuid:    client.ReplicasetUUID,
					ReplicasetName:    client.ReplicasetName,
					ReplicasetVersion: status.ReplicasetVersion,
					RunningDbBackup:   status.RunningDbBackup,
					Compression:       status.CompressionType.String(),
					Encrypted:         status.Cypher.String(),
					Filename:          status.DestinationName,
					BackupType:        status.BackupType.String(),
					StartOplogTs:      status.StartOplogTs,
					LastOplogTs:       status.LastOplogTs,
					LastError:         status.LastError,
					Finished:          status.BackupCompleted,
				},
			}
			if err := stream.Send(c); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *Server) BackupsMetadata(m *pbapi.BackupsMetadataParams, stream pbapi.Api_BackupsMetadataServer) error {
	bmd, err := a.messagesServer.ListBackups()
	if err != nil {
		return errors.Wrap(err, "cannot get backups metadata listing")
	}

	for name := range bmd {
		md := bmd[name]
		msg := &pbapi.MetadataFile{
			Filename: name,
			Metadata: &md,
		}
		if err := stream.Send(msg); err != nil {
			return errors.Wrap(err, "cannot send MetadataFile through the stream")
		}
	}

	return nil
}

// LastBackupMetadata returns the last backup metadata so it can be stored in the local filesystem as JSON
func (a *Server) LastBackupMetadata(ctx context.Context, e *pbapi.LastBackupMetadataParams) (
	*pb.BackupMetadata, error) {
	return a.messagesServer.LastBackupMetadata().Metadata(), nil
}

// StartBackup starts a backup by calling server's StartBackup gRPC method
// This call waits until the backup finish
func (a *Server) RunBackup(ctx context.Context, opts *pbapi.RunBackupParams) (*pbapi.Error, error) {
	msg := &pb.StartBackup{
		OplogStartTime:  time.Now().Unix(),
		BackupType:      pb.BackupType(opts.BackupType),
		CompressionType: pb.CompressionType(opts.CompressionType),
		Cypher:          pb.Cypher(opts.Cypher),
		NamePrefix:      time.Now().UTC().Format(time.RFC3339),
		Description:     opts.Description,
		StorageName:     opts.GetStorageName(),
		// DBBackupName & OplogBackupName are going to be set in server.go
		// We cannot set them here because the backup name will include the replicaset name so, it will
		// be different for each client/MongoDB instance
		// Here we are just using the same pb.StartBackup message to avoid declaring a new structure.
	}

	if err := a.messagesServer.StopBalancer(); err != nil {
		return &pbapi.Error{Message: err.Error()}, err
	}

	a.logger.Debug("Starting the backup")
	if err := a.messagesServer.StartBackup(msg); err != nil {
		return &pbapi.Error{Message: err.Error()}, err
	}
	a.logger.Debug("Backup started")
	a.logger.Debug("Waiting for backup to finish")

	a.messagesServer.WaitBackupFinish()
	a.logger.Debug("Stopping oplog")
	err := a.messagesServer.StopOplogTail()
	if err != nil {
		a.logger.Fatalf("Cannot stop oplog tailer %s", err)
		return &pbapi.Error{Message: err.Error()}, err
	}
	a.logger.Debug("Waiting oplog to finish")
	a.messagesServer.WaitOplogBackupFinish()
	a.logger.Debug("Oplog finished")

	mdFilename := msg.NamePrefix + ".json"

	a.logger.Debugf("Writing metadata to %s", mdFilename)
	if err := a.messagesServer.WriteBackupMetadata(mdFilename); err != nil {
		return &pbapi.Error{Message: err.Error()}, err
	}

	if err := a.messagesServer.StartBalancer(); err != nil {
		return &pbapi.Error{Message: err.Error()}, err
	}
	errs := a.messagesServer.LastBackupErrors()
	pretty.Println(errs)
	return &pbapi.Error{}, nil
}

func (a *Server) RunRestore(ctx context.Context, opts *pbapi.RunRestoreParams) (*pbapi.RunRestoreResponse, error) {
	err := a.messagesServer.RestoreBackupFromMetadataFile(opts.MetadataFile, opts.GetStorageName(), opts.SkipUsersAndRoles)
	if err != nil {
		return &pbapi.RunRestoreResponse{Error: err.Error()}, err
	}

	return &pbapi.RunRestoreResponse{}, nil
}

func (a *Server) ListStorages(opts *pbapi.ListStoragesParams, stream pbapi.Api_ListStoragesServer) error {
	storages, err := a.messagesServer.ListStorages()
	if err != nil {
		return errors.Wrap(err, "cannot get storages from the server")
	}
	for name, stg := range storages {
		msg := &pbapi.StorageInfo{
			Name:          name,
			MatchClients:  stg.MatchClients,
			DifferClients: stg.DifferClients,
			Info: &pb.StorageInfo{
				Valid:    stg.StorageInfo.Valid,
				CanRead:  stg.StorageInfo.CanRead,
				CanWrite: stg.StorageInfo.CanWrite,
				Name:     stg.StorageInfo.Name,
				Type:     stg.StorageInfo.Type,
				S3: &pb.S3{
					Region:      stg.StorageInfo.S3.Region,
					EndpointUrl: stg.StorageInfo.S3.EndpointUrl,
					Bucket:      stg.StorageInfo.S3.Bucket,
				},
				Filesystem: &pb.Filesystem{
					Path: stg.StorageInfo.Filesystem.Path,
				},
			},
		}
		if err := stream.Send(msg); err != nil {
			return errors.Wrap(err, "cannot stream storage info msg for ListStorages")
		}
	}
	return nil
}

func (a *Server) LastBackupErrors(args *pbapi.LastBackupErrorsParams, stream pbapi.Api_LastBackupErrorsServer) error {
	errs := a.messagesServer.LastBackupErrors()
	for _, err := range errs {
		msg := &pbapi.LastBackupError{
			Error: err.Error(),
		}
		if err := stream.Send(msg); err != nil {
			return errors.Wrap(err, "cannot stream last backup errors")
		}
	}

	return nil
}
