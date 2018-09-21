package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	pb "github.com/percona/mongodb-backup/proto/messages"
	"github.com/pkg/errors"
)

type ReplicasetMetadata struct {
	ReplicasetUUID  string `json:"replicaset_uuid"`
	ReplicasetName  string `json:"replicaset_name"`
	DestinationName string `json:"destination_name"`
}

type BackupMetadata struct {
	StartTs         time.Time          `json:"start_ts"`
	EndTs           time.Time          `json:"end_ts"`
	BackupType      pb.BackupType      `json:"backup_type"`
	OplogStartTime  int64              `json:"oplog_start_time"`
	LastOplogTs     int64              `json:"last_oplog_ts"`
	DestinationType pb.DestinationType `json:"destination_type"`
	DestinationDir  string             `json:"destination_dir"`
	Cypher          pb.Cypher          `json:"cypher"`
	CompressionType pb.CompressionType `json:"compression_type"`

	lock        *sync.Mutex                   `json:"-"`
	Replicasets map[string]ReplicasetMetadata `json:"replicas"` // key is replicaset name
}

func NewBackupMetadata() *BackupMetadata {
	return &BackupMetadata{
		Replicasets: make(map[string]ReplicasetMetadata),
		lock:        &sync.Mutex{},
	}
}

func LoadMetadataFromFile(name string) (*BackupMetadata, error) {
	buf, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, err
	}
	metadata := &BackupMetadata{
		Replicasets: make(map[string]ReplicasetMetadata),
		lock:        &sync.Mutex{},
	}
	err = json.Unmarshal(buf, metadata)
	return metadata, nil
}

// AddReplicaset adds backup info for a replicaset using the replicaset name as the key
func (b *BackupMetadata) AddReplicaset(replName, replUUID, destinationName string) error {
	b.lock.Lock()

	if _, ok := b.Replicasets[replName]; ok {
		return fmt.Errorf("Info for replicaset %s already exists", replName)
	}

	b.Replicasets[replName] = ReplicasetMetadata{
		ReplicasetUUID:  replUUID,
		ReplicasetName:  replName,
		DestinationName: destinationName,
	}

	b.lock.Unlock()
	return nil
}

func (b *BackupMetadata) RemoveReplicaset(replName string) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	if _, ok := b.Replicasets[replName]; !ok {
		return fmt.Errorf("Info for replicaset %s doesn't exists", replName)
	}
	delete(b.Replicasets, replName)
	return nil
}

// WriteMetadataToFile writes the backup metadata to a file as JSON
func (b *BackupMetadata) WriteMetadataToFile(name string) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	buf, err := json.Marshal(b)
	if err != nil {
		return errors.Wrap(err, "cannot encode backup metadata")
	}
	if err = ioutil.WriteFile(name, buf, os.ModePerm); err != nil {
		return err
	}
	return nil
}
