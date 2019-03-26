package test_test

import (
	"context"
	"testing"

	"github.com/percona/percona-backup-mongodb/internal/testutils"
	testGrpc "github.com/percona/percona-backup-mongodb/internal/testutils/grpc"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
	log "github.com/sirupsen/logrus"
)

func TestInvalidRestore(t *testing.T) {
	tmpDir := getTempDir(t)
	defer cleanupTempDir(t)

	d, err := testGrpc.NewDaemon(context.Background(), tmpDir, testutils.TestingStorages(), t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}
	defer d.Stop()
	if err := d.StartAllAgents(); err != nil {
		t.Fatalf("Cannot start all agents: %s", err)
	}

	storageName := "invalid-name"

	bm := &pb.BackupMetadata{
		BackupType:      pb.BackupType_BACKUP_TYPE_LOGICAL,
		Cypher:          pb.Cypher_CYPHER_NO_CYPHER,
		CompressionType: pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION,
		Replicasets: map[string]*pb.ReplicasetMetadata{
			"rs1": {
				DbBackupName:    "invalid_file_1.dump",
				OplogBackupName: "invalid_file_1.oplog",
			},
			"rs2": {
				DbBackupName:    "invalid_file_2.dump",
				OplogBackupName: "invalid_file_2.oplog",
			},
		},
		StorageName: storageName,
	}

	if err := d.MessagesServer.RestoreBackUp(bm, storageName, true); err != nil {
		t.Errorf("Cannot restore using backup metadata: %s", err)
	}

	log.Infof("Wating restore to finish")
	d.MessagesServer.WaitRestoreFinish()
}
