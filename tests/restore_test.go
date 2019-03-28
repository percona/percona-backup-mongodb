package test_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
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
	if err := d.MessagesServer.RestoreBackUp(bm, storageName, true); err == nil {
		t.Error("Restore shouldn't start with an invalid storage name")
	}

	storageName = "local-filesystem"
	bm.StorageName = storageName
	if err := d.MessagesServer.RestoreBackUp(bm, storageName, true); err == nil {
		t.Error("Restore shouldn't start with invalid backup file names")
	}

	log.Infof("Wating restore to finish")
	d.MessagesServer.WaitRestoreFinish()
	// if err := d.MessagesServer.WaitRestoreFinish(); err != nil {
	// 	t.Errorf("Backup finished with errors: %s", err)
	// }

	// Now, make the files exist but fill them with invalid data.
	// The restore should also fail.
	bm = &pb.BackupMetadata{
		BackupType:      pb.BackupType_BACKUP_TYPE_LOGICAL,
		Cypher:          pb.Cypher_CYPHER_NO_CYPHER,
		CompressionType: pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION,
		Replicasets: map[string]*pb.ReplicasetMetadata{
			"rs1": {
				DbBackupName:    "file_rs1.dump",
				OplogBackupName: "file_rs1.oplog",
			},
			"rs2": {
				DbBackupName:    "file_rs2.dump",
				OplogBackupName: "file_rs2.oplog",
			},
			"csReplSet": {
				DbBackupName:    "file_cfg.dump",
				OplogBackupName: "file_cfg.oplog",
			},
		},
		StorageName: storageName,
	}

	// create dummy dump & oplog files so CanRestoreBackup func returns true for all shards
	dummyData := "dummy"
	for _, md := range bm.Replicasets {
		fn := filepath.Join(tmpDir, md.DbBackupName)
		if fh, err := os.Create(fn); err != nil {
			t.Fatalf("Cannot create dummy dump file %s: %s", fn, err)
		} else {
			// Write some bytes because CanRestoreBackup will check the file size is != 0
			n, err := fh.Write([]byte(dummyData))
			if err != nil {
				t.Fatalf("Cannot write to %s file: %s", fn, err)
			}
			if n != len(dummyData) {
				t.Fatalf("Cannot write to file %s. Wrote %d bytes instead of %d", fn, n, len(dummyData))
			}
			_ = fh.Close()

			defer os.Remove(fn)
		}
		if _, err := os.Create(filepath.Join(tmpDir, md.OplogBackupName)); err != nil {
			defer os.Remove(filepath.Join(tmpDir, md.OplogBackupName))
		}
	}

	if err := d.MessagesServer.RestoreBackUp(bm, storageName, true); err != nil {
		t.Error("All pre-conditions are valid. Backup should have started")
	}

	log.Infof("Wating restore to finish")
	d.MessagesServer.WaitRestoreFinish()
	if err != nil {
		t.Errorf("Trying to restore invalid files should return errors")
	}
	if err := d.MessagesServer.LastBackupErrors(); err == nil {
		t.Errorf("Trying to restore invalid files should return errors")
	}
}

func TestRestoreOnPrimary(t *testing.T) {
	tmpDir := getTempDir(t)
	//defer cleanupTempDir(t)

	d, err := testGrpc.NewDaemon(context.Background(), tmpDir, testutils.TestingStorages(), t, nil)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}
	defer d.Stop()
	if err := d.StartAllAgents(); err != nil {
		t.Fatalf("Cannot start all agents: %s", err)
	}

	storageName := "local-filesystem"

	bm := &pb.BackupMetadata{
		BackupType:      pb.BackupType_BACKUP_TYPE_LOGICAL,
		Cypher:          pb.Cypher_CYPHER_NO_CYPHER,
		CompressionType: pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION,
		Replicasets: map[string]*pb.ReplicasetMetadata{
			"rs1": {
				DbBackupName:    "file_rs1.dump",
				OplogBackupName: "file_rs1.oplog",
			},
			"rs2": {
				DbBackupName:    "file_rs2.dump",
				OplogBackupName: "file_rs2.oplog",
			},
			"csReplSet": {
				DbBackupName:    "file_cfg.dump",
				OplogBackupName: "file_cfg.oplog",
			},
		},
		StorageName: storageName,
	}
	// create dummy dump & oplog files so CanRestoreBackup func returns true for all shards
	dummyData := "dummy"
	for _, md := range bm.Replicasets {
		fmt.Println(filepath.Join(tmpDir, md.DbBackupName))
		fn := filepath.Join(tmpDir, md.DbBackupName)
		if fh, err := os.Create(fn); err != nil {
			t.Fatalf("Cannot create dummy dump file %s: %s", fn, err)
		} else {
			// Write some bytes because CanRestoreBackup will check the file size is != 0
			n, err := fh.Write([]byte(dummyData))
			if err != nil {
				t.Fatalf("Cannot write to %s file: %s", fn, err)
			}
			if n != len(dummyData) {
				t.Fatalf("Cannot write to file %s. Wrote %d bytes instead of %d", fn, n, len(dummyData))
			}
			_ = fh.Close()

			defer os.Remove(fn)
		}
		if _, err := os.Create(filepath.Join(tmpDir, md.OplogBackupName)); err != nil {
			defer os.Remove(filepath.Join(tmpDir, md.OplogBackupName))
		}
	}

	sources, err := d.MessagesServer.RestoreSourcesByReplicaset(bm, storageName)
	if err != nil {
		t.Errorf("Cannot get RestoreSourcesByReplicaset: %s", err)
	}
	got := []string{}
	want := []string{"127.0.0.1:17001", "127.0.0.1:17004", "127.0.0.1:17007"}
	for _, src := range sources {
		got = append(got, fmt.Sprintf("%s:%s", src.Host, src.Port))
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Sources don't match. Got: %v, want: %v", got, want)
	}
}
