package test_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/percona/percona-backup-mongodb/grpc/server"
	"github.com/percona/percona-backup-mongodb/internal/testutils"
	testGrpc "github.com/percona/percona-backup-mongodb/internal/testutils/grpc"
	pbapi "github.com/percona/percona-backup-mongodb/proto/api"
)

// Why this is not in the grpc/api dir? Because the daemon implemented to test the whole behavior
// imports proto/api so if we try to use the daemon from the api package we would end up having
// cycling imports.
func TestApiWithDaemon(t *testing.T) {
	tmpDir := getCleanTempDir(t)

	d, err := testGrpc.NewDaemon(context.Background(), tmpDir, testutils.TestingStorages(), t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}
	if err := d.StartAllAgents(); err != nil {
		t.Fatalf("Cannot start all agents: %s", err)
	}
	defer d.Stop()

	msg := &pbapi.RunBackupParams{
		BackupType:      pbapi.BackupType_BACKUP_TYPE_LOGICAL,
		CompressionType: pbapi.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION,
		Cypher:          pbapi.Cypher_CYPHER_NO_CYPHER,
		Description:     "test backup",
		StorageName:     "local-filesystem",
	}

	_, err = d.APIServer.RunBackup(context.Background(), msg)
	if err != nil {
		t.Fatalf("Cannot start backup from API: %s", err)
	}

	// Check we have consistent backup names
	dir, err := os.Open(tmpDir)
	if err != nil {
		t.Fatalf("Cannot open backup directory: %s", err)
	}
	names, err := dir.Readdirnames(0)
	if err != nil {
		t.Fatalf("Cannot list backup directory files: %s", err)
	}
	if len(names) < 2 {
		t.Fatalf("Invalid backup files count. It should be > 2, got %d", len(names))
	}

	// get the prefix (timestamp). Notice that the .json files doesn't have an _
	p := strings.Split(names[0], "_")
	if len(p) < 2 {
		// maybe it is the json file ...
		p = strings.Split(names[0], ".")
		if len(p) < 2 {
			t.Errorf("Invalid backup file names prefix. It should be date_shard, got %s", names[0])
		}
	}

	prefix := p[0]
	jsonFile := prefix + ".json"
	metadataFound := false

	for _, name := range names {
		if name == jsonFile {
			metadataFound = true
		}
		if !strings.HasPrefix(name, prefix) {
			t.Errorf("Invalid backup file name. It should start with %q, got %q", prefix, name)
		}
	}

	if !metadataFound {
		t.Errorf("Metadata file %s was not found", jsonFile)
	}

	metadataFile := filepath.Join(tmpDir, jsonFile)
	md, err := server.LoadMetadataFromFile(metadataFile)
	if err != nil {
		t.Fatalf("Cannot load metadata from file %s: %v", metadataFile, err)
	}

	if md.Metadata().Description == "" {
		t.Errorf("Empty description in backup metadata")
	}

	if len(md.Metadata().Replicasets) < 3 {
		t.Errorf("Invalid replicasets count in metadata. Want 3, got %d", len(md.Metadata().Replicasets))
	}

	mrs1, ok := md.Metadata().Replicasets["rs1"]
	if !ok {
		t.Errorf("Missing rs1 in backup metadata")
	} else if mrs1.GetClusterId() == "" {
		t.Errorf("Missing cluster ID for replicaset 1")
	}

	mdStream := newMockBackupsMetadataStream()

	err = d.APIServer.BackupsMetadata(&pbapi.BackupsMetadataParams{}, mdStream)
	if err != nil {
		t.Errorf("Cannot get backups metadata: %s", err)
	}

	if len(mdStream.files) != 1 {
		t.Errorf("Invalid backup metadata files list. Want 1 file, got %d", len(mdStream.files))
	}

	if jf, ok := mdStream.files[jsonFile]; !ok {
		t.Errorf("%s file entry is missing", jsonFile)
	} else if jf.Description != msg.Description {
		t.Errorf("Invalid backup description. Want %q, got %q", msg.Description, jf.Description)
	}
}

func TestBackupFail(t *testing.T) {
	tmpDir := getTempDir(t)

	d, err := testGrpc.NewDaemon(context.Background(), tmpDir, testutils.TestingStorages(), t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}
	if err := d.StartAllAgents(); err != nil {
		t.Fatalf("Cannot start all agents: %s", err)
	}
	defer d.Stop()

	msg := &pbapi.RunBackupParams{
		BackupType:      pbapi.BackupType_BACKUP_TYPE_LOGICAL,
		CompressionType: pbapi.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION,
		Cypher:          pbapi.Cypher_CYPHER_NO_CYPHER,
		Description:     "test backup",
		StorageName:     "invalid-storage-name",
	}

	_, err = d.APIServer.RunBackup(context.Background(), msg)
	if err == nil {
		t.Error("Backup shouldn't start with an invalid storage")
	}
}
