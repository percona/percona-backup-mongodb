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

type BackupMetadata struct {
	metadata *pb.BackupMetadata
	lock     *sync.Mutex `json:"-"`
}

func NewBackupMetadata(opts *pb.StartBackup) *BackupMetadata {
	return &BackupMetadata{
		metadata: &pb.BackupMetadata{
			StartTs:         time.Now().UTC().Unix(),
			BackupType:      opts.GetBackupType(),
			DestinationType: opts.GetDestinationType(),
			DestinationDir:  opts.GetDestinationDir(),
			CompressionType: opts.GetCompressionType(),
			Cypher:          opts.GetCypher(),
			Replicasets:     make(map[string]*pb.ReplicasetMetadata),
		},
		lock: &sync.Mutex{},
	}
}

// AddReplicaset adds backup info for a replicaset using the replicaset name as the key
func (b *BackupMetadata) AddReplicaset(replName, replUUID, dbBackupName, oplogBackupName string) error {
	b.lock.Lock()

	if _, ok := b.metadata.Replicasets[replName]; ok {
		return fmt.Errorf("Info for replicaset %s already exists", replName)
	}

	// Key is replicaset name instead of UUID because the UUID is randomly generated so, on a
	// new and shiny environment created to restore a backup, the UUID will be different.
	// On restore, we will try to restore each replicaset by name to the matching cluster.
	b.metadata.Replicasets[replName] = &pb.ReplicasetMetadata{
		ReplicasetUUID:  replUUID,
		ReplicasetName:  replName,
		DBBackupName:    dbBackupName,
		OplogBackupName: oplogBackupName,
	}

	b.lock.Unlock()
	return nil
}

func LoadMetadataFromFile(name string) (*BackupMetadata, error) {
	buf, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, err
	}
	metadata := &BackupMetadata{
		metadata: &pb.BackupMetadata{
			Replicasets: make(map[string]*pb.ReplicasetMetadata),
		},
		lock: &sync.Mutex{},
	}
	err = json.Unmarshal(buf, &metadata.metadata)
	return metadata, nil
}

func (b *BackupMetadata) Metadata() *pb.BackupMetadata {
	return b.metadata
}

func (b *BackupMetadata) RemoveReplicaset(replName string) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	if _, ok := b.metadata.Replicasets[replName]; !ok {
		return fmt.Errorf("Info for replicaset %s doesn't exists", replName)
	}
	delete(b.metadata.Replicasets, replName)
	return nil
}

// WriteMetadataToFile writes the backup metadata to a file as JSON
func (b *BackupMetadata) WriteMetadataToFile(name string) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	buf, err := json.MarshalIndent(b.metadata, "", "    ")
	if err != nil {
		return errors.Wrap(err, "cannot encode backup metadata")
	}
	if err = ioutil.WriteFile(name, buf, os.ModePerm); err != nil {
		return err
	}
	return nil
}
