package server

import (
	"context"
	"fmt"
	"sync"

	pb "github.com/percona/mongodb-backup/proto/messages"
)

type MessagesServer struct {
	stopChan chan struct{}
	lock     *sync.Mutex
	clients  map[string]*Client
}

func NewMessagesServer() *MessagesServer {
	messagesServer := &MessagesServer{
		lock:     &sync.Mutex{},
		clients:  make(map[string]*Client),
		stopChan: make(chan struct{}),
	}
	return messagesServer
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

func (s *MessagesServer) BackupCompleted(ctx context.Context, msg *pb.BackupCompletedMsg) (*pb.AckMsg, error) {
	return nil, fmt.Errorf("BackupCompleted() not implemented yet")
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
