package api

import (
	"github.com/percona/mongodb-backup/grpc/server"
	pbapi "github.com/percona/mongodb-backup/proto/api"
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
	for _, client := range a.messagesServer.Clients() {
		c := &pbapi.Client{
			ClientID: client.ID,
			Status:   &pbapi.ClientStatus{},
			LastSeen: client.LastSeen.Unix(),
		}
		stream.Send(c)
	}
	return nil
}
