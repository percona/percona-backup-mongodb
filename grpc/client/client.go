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
	nodeType   string
	grpcClient pb.MessagesClient
	inMsgChan  chan *pb.ServerMessage
	outMsgChan chan *pb.ClientMessage
	stopChan   chan struct{}
	stream     pb.Messages_MessagesChatClient
	//
	waitg   *sync.WaitGroup
	lock    *sync.Mutex
	running bool
}

func NewClient(id string, clusterID *bson.ObjectId, nodeType string, grpcClient pb.MessagesClient) (*Client, error) {
	if id == "" {
		return nil, fmt.Errorf("ClientID cannot be empty")
	}
	stream, err := grpcClient.MessagesChat(context.Background())
	if err != nil {
		return nil, err
	}

	m := &pb.ClientMessage{
		Type:     pb.ClientMessage_REGISTER,
		ClientID: id,
		Payload: &pb.ClientMessage_RegisterMsg{
			RegisterMsg: &pb.RegisterPayload{
				NodeType:  nodeType,
				ClusterID: clusterID.Hex(),
			},
		},
	}

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

	return &Client{
		id:         id,
		nodeType:   nodeType,
		grpcClient: grpcClient,
		inMsgChan:  make(chan *pb.ServerMessage),
		outMsgChan: make(chan *pb.ClientMessage),
		stream:     stream,
	}, nil
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

	c.waitg.Add(1)
	go func() {
		for {
			in, err := c.stream.Recv()
			if err == io.EOF {
				c.inMsgChan <- nil
				c.waitg.Done()
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

	c.stream.CloseSend()
	c.waitg.Wait()
	c.running = false
}
