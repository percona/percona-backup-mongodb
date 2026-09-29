package etcd

import (
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const dialTimeout = 5 * time.Second

// NewClient connects to the ctrl-agents' etcd as a remote client.
func NewClient(endpoints []string) (*clientv3.Client, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no etcd endpoints configured")
	}

	return clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	})
}
