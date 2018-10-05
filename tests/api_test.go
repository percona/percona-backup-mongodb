package test_test

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/percona/mongodb-backup/grpc/server"
	"github.com/percona/mongodb-backup/internal/testutils"
	pbapi "github.com/percona/mongodb-backup/proto/api"
	log "github.com/sirupsen/logrus"
)

// Why this is not in the grpc/api dir? Because the daemon implemented to test the whole behavior
// imports proto/api so if we try to use the daemon from the api package we would end up having
// cycling imports.
func TestApiWithDaemon(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("Cannot create temp dir %s: %s", tmpDir, err)
	}
	log.Printf("Using %s as the temporary directory", tmpDir)
	defer os.RemoveAll(tmpDir) // Clean up after testing.

	d, err := testutils.NewGrpcDaemon(context.Background(), tmpDir, t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}

	msg := &pbapi.RunBackupParams{
		BackupType:      pbapi.BackupType_LOGICAL,
		DestinationType: pbapi.DestinationType_FILE,
		CompressionType: pbapi.CompressionType_NO_COMPRESSION,
		Cypher:          pbapi.Cypher_NO_CYPHER,
		Description:     "test backup",
	}

	_, err = d.ApiServer.RunBackup(context.Background(), msg)
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

	md, err := server.LoadMetadataFromFile(filepath.Join(tmpDir, jsonFile))
	if md.Metadata().Description == "" {
		t.Errorf("Empty description in backup metadata")
	}

	if len(md.Metadata().Replicasets) < 3 {
		t.Errorf("Invalid replicasets count in metadata. Want 3, got %d", len(md.Metadata().Replicasets))
	}

	d.Stop()
}
