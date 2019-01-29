package test_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/percona/percona-backup-mongodb/grpc/server"
	testGrpc "github.com/percona/percona-backup-mongodb/internal/testutils/grpc"
	pbapi "github.com/percona/percona-backup-mongodb/proto/api"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
	"google.golang.org/grpc/metadata"
)

// Mock stream interface to avoid connecting to the API server via TCP
// We just need to test the methods but the server is running locally
type mockBackupsMetadataStream struct {
	files map[string]*pb.BackupMetadata
}

func newMockBackupsMetadataStream() *mockBackupsMetadataStream {
	return &mockBackupsMetadataStream{
		files: make(map[string]*pb.BackupMetadata),
	}
}

func (m *mockBackupsMetadataStream) SendMsg(imsg interface{}) error {
	msg := imsg.(*pbapi.MetadataFile)
	m.files[msg.Filename] = msg.Metadata
	return nil
}

func (m *mockBackupsMetadataStream) Send(msg *pbapi.MetadataFile) error {
	m.files[msg.Filename] = msg.Metadata
	return nil
}

func (m *mockBackupsMetadataStream) Context() context.Context       { return context.TODO() }
func (m *mockBackupsMetadataStream) RecvMsg(im interface{}) error   { return nil }
func (m *mockBackupsMetadataStream) SetHeader(h metadata.MD) error  { return nil }
func (m *mockBackupsMetadataStream) SendHeader(h metadata.MD) error { return nil }
func (m *mockBackupsMetadataStream) SetTrailer(h metadata.MD)       {}

// Why this is not in the grpc/api dir? Because the daemon implemented to test the whole behavior
// imports proto/api so if we try to use the daemon from the api package we would end up having
// cycling imports.
func TestApiWithDaemon(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "dump_test") // same dir we have in testingStorages()
	if err := os.MkdirAll(tmpDir, os.ModePerm); err != nil {
		t.Fatalf("Cannot create temp dir %q for backup: %s", tmpDir, err)
	}
	defer os.RemoveAll(tmpDir) // Clean up after testing.

	d, err := testGrpc.NewDaemon(context.Background(), tmpDir, testingStorages(), t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}
	d.StartAllAgents()
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
	} else {
		if mrs1.GetClusterId() == "" {
			t.Errorf("Missing cluster ID for replicaset 1")
		}
	}

	stream := newMockBackupsMetadataStream()

	err = d.APIServer.BackupsMetadata(&pbapi.BackupsMetadataParams{}, stream)
	if err != nil {
		t.Errorf("Cannot get backups metadata: %s", err)
	}

	if len(stream.files) != 1 {
		t.Errorf("Invalid backup metadata files list. Want 1 file, got %d", len(stream.files))
	}

	if jf, ok := stream.files[jsonFile]; !ok {
		t.Errorf("%s file entry is missing", jsonFile)
	} else {
		if jf.Description != msg.Description {
			t.Errorf("Invalid backup description. Want %q, got %q", msg.Description, jf.Description)
		}
	}
}
