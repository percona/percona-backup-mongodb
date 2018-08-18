package server

import (
	"fmt"
	"sync"
	"time"

	"github.com/percona/mongodb-backup/internal/notify"
	pb "github.com/percona/mongodb-backup/proto/messages"
)

var (
	ClientAlreadyExistsError = fmt.Errorf("Client ID already registered")
	UnknownClientID          = fmt.Errorf("Unknown client ID")
)

type ClientStatus struct {
	ReplicaSetUUID    string    `json:"replica_set_uuid"`
	ReplicaSetName    string    `json:"replica_set_name"`
	ReplicaSetVersion int64     `json:"replica_set_version"`
	LastOplogTime     time.Time `json:"last_oplog_time"`
	//
	RunningDBBackup    bool      `json:"running_db_backup"`
	RunningOplogBackup bool      `json:"running_oplog_backup"`
	Compression        string    `json:"compression"`
	Encrypted          string    `json:"encrypted"`
	Destination        string    `json:"destination"`
	Filename           string    `json:"filename"`
	Started            time.Time `json:"started"`
	Finished           time.Time `json:"finished"`
	LastError          string    `json:"last_error"`
}

type Client struct {
	ID              string       `json:"id"`
	NodeType        string       `json:"node_type"`
	LastCommandSent int          `json:"last_command_ent"`
	LastSeen        time.Time    `json:"last_seen"`
	Status          ClientStatus `json:"Status"`
	//
	inMessagesChan  chan *pb.ClientMessage
	outMessagesChan chan *pb.ServerMessage
	pongChan        chan time.Time
	lock            *sync.Mutex
	streaming       bool
}

func NewClient(id string, nodeType pb.NodeType) *Client {
	client := &Client{
		ID:       id,
		lock:     &sync.Mutex{},
		pongChan: make(chan time.Time),
		LastSeen: time.Now(),
	}
	return client
}

func (c *Client) IsStreaming() bool {
	return c.streaming
}

func (c *Client) InMsgChan() chan *pb.ClientMessage {
	return c.inMessagesChan
}

func (c *Client) OutMsgChan() chan *pb.ServerMessage {
	return c.outMessagesChan
}

func (c *Client) StartStreamIO(stream pb.Messages_MessagesChatServer) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.streaming {
		return fmt.Errorf("Already reading from the stream")
	}

	c.inMessagesChan = make(chan *pb.ClientMessage)
	c.outMessagesChan = make(chan *pb.ServerMessage, 100)
	c.streaming = true

	go func() {
		for {
			in, err := stream.Recv()
			if err != nil {
				// A nil message will signal the process to stop on the server
				c.inMessagesChan <- nil
				return
			}
			notify.PostTimeout(in.Type, time.Now(), 1*time.Millisecond)
			c.inMessagesChan <- in
		}
	}()

	go func() {
		for {
			msg, ok := <-c.outMessagesChan
			if !ok {
				return
			}
			if err := stream.Send(msg); err != nil {
				return
			}
		}
	}()
	return nil
}

func (c *Client) StopStreamIO() {
	c.lock.Lock()
	defer c.lock.Unlock()

	if !c.streaming {
		return
	}

	c.streaming = false
	close(c.outMessagesChan)
}

func (c *Client) Ping() (time.Time, error) {
	pongChan := notify.Start(pb.ClientMessage_PONG)
	defer notify.Stop(pb.ClientMessage_PONG, pongChan)

	c.outMessagesChan <- &pb.ServerMessage{Type: pb.ServerMessage_PING}

	// wait for pong
	select {
	case <-time.After(2 * time.Second):
		return time.Time{}, fmt.Errorf("Ping timeout")
	case pongTS := <-c.pongChan:
		return pongTS, nil
	}
}

func (c *Client) SendMsg(msg *pb.ServerMessage) error {
	if !c.streaming {
		return fmt.Errorf("not streaming")
	}
	c.outMessagesChan <- msg
	return nil
}
