package client

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/globalsign/mgo/bson"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"github.com/pkg/errors"
)

type Client struct {
	id         string
	clusterID  *bson.ObjectId
	nodeType   pb.NodeType
	grpcClient pb.MessagesClient
	inMsgChan  chan *pb.ServerMessage
	outMsgChan chan *pb.ClientMessage
	stopChan   chan struct{}
	stream     pb.Messages_MessagesChatClient
	ctxCancel  context.CancelFunc
	//
	waitg   *sync.WaitGroup
	lock    *sync.Mutex
	running bool
}

func NewClient(id, name string, clusterID, replicasetID *bson.ObjectId, nodeType pb.NodeType, grpcClient pb.MessagesClient) (*Client, error) {
	if id == "" {
		return nil, fmt.Errorf("ClientID cannot be empty")
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := grpcClient.MessagesChat(ctx)
	if err != nil {
		cancel()
		return nil, err
	}

	clusterIDString := ""
	if clusterID != nil {
		clusterIDString = clusterID.Hex()
	}

	m := &pb.ClientMessage{
		Type:     pb.ClientMessage_REGISTER,
		ClientID: id,
		Payload: &pb.ClientMessage_RegisterMsg{
			RegisterMsg: &pb.RegisterPayload{
				NodeType:       nodeType,
				ClusterID:      clusterIDString,
				ReplicasetName: name,
				ReplicasetID:   replicasetID.Hex(),
			},
		},
	}

	if err := stream.Send(m); err != nil {
		cancel()
		return nil, errors.Wrap(err, "Failed to send registration message")
	}

	response, err := stream.Recv()
	if err != nil {
		cancel()
		return nil, errors.Wrap(err, "Error while receiving the registration response from the server")
	}
	if response.Type != pb.ServerMessage_REGISTRATION_OK {
		cancel()
		return nil, fmt.Errorf("Invalid registration response type: %d", response.Type)
	}

	c := &Client{
		id:         id,
		nodeType:   nodeType,
		grpcClient: grpcClient,
		inMsgChan:  make(chan *pb.ServerMessage),
		outMsgChan: make(chan *pb.ClientMessage),
		stream:     stream,
		lock:       &sync.Mutex{},
		waitg:      &sync.WaitGroup{},
		ctxCancel:  cancel,
	}
	return c, nil
}

func (c *Client) NodeType() pb.NodeType {
	return c.nodeType
}

func (c *Client) OutMsgChan() chan<- *pb.ClientMessage {
	return c.outMsgChan
}

func (c *Client) InMsgChan() <-chan *pb.ServerMessage {
	return c.inMsgChan
}

func (c *Client) StartStreamIO() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.running = true
	c.stopChan = make(chan struct{})

	go func() {
		for {
			in, err := c.stream.Recv()
			if err == io.EOF {
				c.inMsgChan <- nil
				close(c.stopChan)
				return
			}
			c.inMsgChan <- in
		}
	}()

	c.waitg.Add(1)
	go func() {
		for {
			select {
			case <-c.stopChan:
				c.waitg.Done()
				return
			case msg := <-c.outMsgChan:
				c.stream.Send(msg)
			}
		}
	}()

	return nil
}

func (c *Client) StopStreamIO() {
	c.lock.Lock()
	defer c.lock.Unlock()

	if !c.running {
		return
	}

	c.ctxCancel()
	c.stream.CloseSend()
	close(c.stopChan)
	c.waitg.Wait()
	c.running = false
}
