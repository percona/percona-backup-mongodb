package api

import (
	"context"
	"path/filepath"

	"github.com/percona/mongodb-backup/grpc/server"
	pbapi "github.com/percona/mongodb-backup/proto/api"
	"github.com/pkg/errors"
)

type ApiServer struct {
	messagesServer *server.MessagesServer
}

func NewApiServer(server *server.MessagesServer) *ApiServer {
	return &ApiServer{
		messagesServer: server,
	}
}

func (a *ApiServer) GetClients(m *pbapi.Empty, stream pbapi.Api_GetClientsServer) error {
	for _, clients := range a.messagesServer.GetClientsStatus() {
		for _, client := range clients {
			status, err := client.Status()
			if err != nil {
				return errors.Wrapf(err, "cannot get the status for client %q", client.ID)
			}

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

func (a *ApiServer) StartBackup(context.Context, *pbapi.StartBackupParams) (*pbapi.StartBackupResponse, error) {

	return nil, nil
}
