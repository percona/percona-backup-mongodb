package slicer

import (
	"context"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	pbmlog "github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
	"github.com/percona/percona-backup-mongodb/pbm/storage/fs"
)

var (
	mClient    *mongo.Client
	connClient connect.Client
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	mongodbContainer, err := mongodb.Run(ctx, "perconalab/percona-server-mongodb:8.0.4-multi")
	if err != nil {
		log.Fatalf("error while creating mongo test container: %v", err)
	}
	connStr, err := mongodbContainer.ConnectionString(ctx)
	if err != nil {
		log.Fatalf("conn string error: %v", err)
	}
	mClient, err = mongo.Connect(ctx, options.Client().ApplyURI(connStr))
	if err != nil {
		log.Fatalf("mongo client connect error: %v", err)
	}

	connClient = connect.UnsafeClient(mClient)

	code := m.Run()

	err = mClient.Disconnect(ctx)
	if err != nil {
		log.Fatalf("mongo client disconnect error: %v", err)
	}
	if err := testcontainers.TerminateContainer(mongodbContainer); err != nil {
		log.Fatalf("failed to terminate container: %s", err)
	}

	os.Exit(code)
}

func TestCatchup(t *testing.T) {
	ctx := context.Background()

	t.Run("no base backup", func(t *testing.T) {
		s := createTestSlicer(t)

		err := s.Catchup(ctx)
		if err == nil {
			t.Fatal("expected error when there's no base backup")
		}
		wantErr := "full backup is required to start PITR"
		if !strings.Contains(err.Error(), wantErr) {
			t.Fatalf("not base backup error, want=%v, got=%v", wantErr, err)
		}
	})

	t.Run("base backup without RS data", func(t *testing.T) {
		s := createTestSlicer(t)

		backupMeta := createBackupMeta()
		backupMeta.Replsets = []backup.BackupReplset{
			{
				Name:         "rsX",
				FirstWriteTS: primitive.Timestamp{T: 500, I: 0},
				LastWriteTS:  primitive.Timestamp{T: 1000, I: 0},
			},
		}
		_, err := connClient.BcpCollection().InsertOne(ctx, backupMeta)
		if err != nil {
			t.Fatalf("failed to insert backup: %v", err)
		}

		err = s.Catchup(ctx)
		if err == nil {
			t.Fatal("expected error when RS is not part of base backup")
		}
		if !strings.Contains(err.Error(), "no replset") {
			t.Fatalf("expected 'no replset' error, got: %v", err)
		}
	})

	t.Run("no existing PITR chunks, use last write from backup", func(t *testing.T) {
		s := createTestSlicer(t)

		wantLastTS := primitive.Timestamp{T: 1000, I: 0}
		backupMeta := createBackupMeta()
		backupMeta.LastWriteTS = wantLastTS
		backupMeta.Replsets[0].LastWriteTS = wantLastTS
		_, err := connClient.BcpCollection().InsertOne(ctx, backupMeta)
		if err != nil {
			t.Fatalf("failed to insert backup: %v", err)
		}

		err = s.Catchup(ctx)
		if err != nil {
			t.Fatalf("unexpected error in catchup: %v", err)
		}
		if s.lastTS != wantLastTS {
			t.Fatalf("expected lastTS to be %v, got %v", wantLastTS, s.lastTS)
		}
	})

	t.Run("no base backup after restore", func(t *testing.T) {
		s := createTestSlicer(t)

		backupMeta := createBackupMeta()
		backupMeta.StartTS = 1000
		backupMeta.LastWriteTS = primitive.Timestamp{T: 1000, I: 0}
		backupMeta.Replsets[0].LastWriteTS = primitive.Timestamp{T: 1000, I: 0}
		_, err := connClient.BcpCollection().InsertOne(ctx, backupMeta)
		if err != nil {
			t.Fatalf("failed to insert backup: %v", err)
		}

		restoreMeta := &restore.RestoreMeta{
			Name:    "restore1",
			Backup:  "2025-11-21T09:38:09Z",
			StartTS: 2000,
			Status:  defs.StatusDone,
		}
		_, err = connClient.RestoresCollection().InsertOne(ctx, restoreMeta)
		if err != nil {
			t.Fatalf("failed to insert restore: %v", err)
		}

		err = s.Catchup(ctx)
		if err == nil {
			t.Fatal("expected error when there's no base backup after restore")
		}
		wantErr := "no backup found after the restored"
		if !strings.Contains(err.Error(), wantErr) {
			t.Fatalf("expected %q error, got: %v", wantErr, err)
		}
	})

	t.Run("chunk after backup", func(t *testing.T) {
		s := createTestSlicer(t)

		lastWriteTS := primitive.Timestamp{T: 1000, I: 0}
		backupMeta := createBackupMeta()
		backupMeta.LastWriteTS = lastWriteTS
		backupMeta.Replsets[0].LastWriteTS = lastWriteTS
		_, err := connClient.BcpCollection().InsertOne(ctx, backupMeta)
		if err != nil {
			t.Fatalf("failed to insert backup: %v", err)
		}

		wantChunkEndTS := primitive.Timestamp{T: 1500, I: 0}
		chunk := &oplog.OplogChunk{
			RS:          "rs0",
			FName:       "chunk1",
			Compression: compress.CompressionTypeNone,
			StartTS:     primitive.Timestamp{T: 1000, I: 0},
			EndTS:       wantChunkEndTS,
			Size:        1024,
		}
		_, err = connClient.PITRChunksCollection().InsertOne(ctx, chunk)
		if err != nil {
			t.Fatalf("failed to insert chunk: %v", err)
		}

		err = s.Catchup(ctx)
		if err != nil {
			t.Fatalf("unexpected error in catchup: %v", err)
		}
		if s.lastTS != wantChunkEndTS {
			t.Fatalf("expected lastTS to be %v, got %v", wantChunkEndTS, s.lastTS)
		}
	})

	t.Run("restore after last chunk", func(t *testing.T) {
		s := createTestSlicer(t)

		wantLastTS := primitive.Timestamp{T: 1000, I: 0}
		backupMeta := createBackupMeta()
		backupMeta.LastWriteTS = wantLastTS
		backupMeta.Replsets[0].LastWriteTS = wantLastTS
		_, err := connClient.BcpCollection().InsertOne(ctx, backupMeta)
		if err != nil {
			t.Fatalf("failed to insert backup: %v", err)
		}

		chunkEndTS := primitive.Timestamp{T: 800, I: 0}
		chunk := &oplog.OplogChunk{
			RS:          "rs0",
			FName:       "chunk1",
			Compression: compress.CompressionTypeNone,
			StartTS:     primitive.Timestamp{T: 500, I: 0},
			EndTS:       chunkEndTS,
			Size:        1024,
		}
		_, err = connClient.PITRChunksCollection().InsertOne(ctx, chunk)
		if err != nil {
			t.Fatalf("failed to insert chunk: %v", err)
		}

		// Create a restore after chunk
		restoreMeta := &restore.RestoreMeta{
			Name:    "restore1",
			Backup:  "2025-11-21T09:38:09Z",
			StartTS: 900,
			Status:  defs.StatusDone,
		}

		_, err = connClient.RestoresCollection().InsertOne(ctx, restoreMeta)
		if err != nil {
			t.Fatalf("failed to insert restore: %v", err)
		}

		err = s.Catchup(ctx)
		if err != nil {
			t.Fatalf("unexpected error in catchup: %v", err)
		}
		if s.lastTS != wantLastTS {
			t.Fatalf("expected lastTS to be %v, got %v", wantLastTS, s.lastTS)
		}
	})

	t.Run("chunk between backup timestamps", func(t *testing.T) {
		s := createTestSlicer(t)

		backupFirstTS := primitive.Timestamp{T: 500, I: 0}
		wantLastTS := primitive.Timestamp{T: 1000, I: 0}
		backupMeta := createBackupMeta()
		backupMeta.LastWriteTS = wantLastTS
		backupMeta.Replsets[0].FirstWriteTS = backupFirstTS
		backupMeta.Replsets[0].LastWriteTS = wantLastTS
		backupMeta.Replsets[0].OplogName = "2025-11-21T09:38:09Z/oplog"
		_, err := connClient.BcpCollection().InsertOne(ctx, backupMeta)
		if err != nil {
			t.Fatalf("failed to insert backup: %v", err)
		}

		// Create a chunk with endTS between backup FirstWriteTS and LastWriteTS
		chunkEndTS := primitive.Timestamp{T: 750, I: 0}
		chunk := &oplog.OplogChunk{
			RS:          "rs0",
			FName:       "chunk1",
			Compression: compress.CompressionTypeNone,
			StartTS:     primitive.Timestamp{T: 600, I: 0},
			EndTS:       chunkEndTS,
			Size:        1024,
		}
		_, err = connClient.PITRChunksCollection().InsertOne(ctx, chunk)
		if err != nil {
			t.Fatalf("failed to insert chunk: %v", err)
		}

		cfg := &config.Config{
			PITR: &config.PITRConf{
				Compression: compress.CompressionTypeNone,
			},
		}
		_, err = connClient.ConfigCollection().InsertOne(ctx, cfg)
		if err != nil {
			t.Fatalf("failed to insert config: %v", err)
		}

		err = s.Catchup(ctx)
		if err != nil {
			t.Fatalf("unexpected error in catchup: %v", err)
		}
		if s.lastTS != wantLastTS {
			t.Fatalf("expected lastTS to be %v, got %v", wantLastTS, s.lastTS)
		}
	})

	t.Run("chunk before backup first write", func(t *testing.T) {
		s := createTestSlicer(t)

		backupFirstTS := primitive.Timestamp{T: 1000, I: 0}
		backupLastTS := primitive.Timestamp{T: 1500, I: 0}
		backupMeta := createBackupMeta()
		backupMeta.LastWriteTS = backupLastTS
		backupMeta.Replsets[0].FirstWriteTS = backupFirstTS
		backupMeta.Replsets[0].LastWriteTS = backupLastTS
		backupMeta.Replsets[0].OplogName = "2025-11-21T09:38:09Z/rs0/oplog"

		_, err := connClient.BcpCollection().InsertOne(ctx, backupMeta)
		if err != nil {
			t.Fatalf("failed to insert backup: %v", err)
		}

		// Create a chunk with endTS before backup FirstWriteTS
		chunkEndTS := primitive.Timestamp{T: 500, I: 0}
		chunk := &oplog.OplogChunk{
			RS:          "rs0",
			FName:       "chunk1",
			Compression: compress.CompressionTypeNone,
			StartTS:     primitive.Timestamp{T: 200, I: 0},
			EndTS:       chunkEndTS,
			Size:        1024,
		}

		_, err = connClient.PITRChunksCollection().InsertOne(ctx, chunk)
		if err != nil {
			t.Fatalf("failed to insert chunk: %v", err)
		}

		cfg := &config.Config{
			PITR: &config.PITRConf{
				Compression: compress.CompressionTypeNone,
			},
		}
		_, err = connClient.ConfigCollection().InsertOne(ctx, cfg)
		if err != nil {
			t.Fatalf("failed to insert config: %v", err)
		}

		err = s.Catchup(ctx)
		if err != nil {
			t.Fatalf("unexpected error in catchup: %v", err)
		}
		if s.lastTS != backupLastTS {
			t.Fatalf("expected lastTS to be %v, got %v", backupLastTS, s.lastTS)
		}
	})
}

func createTestSlicer(t *testing.T) *Slicer {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "pbm-test-storage-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	t.Cleanup(func() {
		cleanupControlColl(t)
		os.RemoveAll(tmpDir)
	})

	stg, err := fs.New(&fs.Config{Path: tmpDir})
	if err != nil {
		t.Fatalf("failed to create filesystem storage: %v", err)
	}
	cfg := &config.Config{
		Epoch: primitive.Timestamp{T: 1, I: 0},
		PITR: &config.PITRConf{
			Compression: compress.CompressionTypeNone,
		},
	}

	return NewSlicer("rs0", connClient, mClient, stg, cfg, pbmlog.DiscardLogger)
}

func createBackupMeta() *backup.BackupMeta {
	lastWriteTS := primitive.Timestamp{T: 1000, I: 0}
	return &backup.BackupMeta{
		Name:        "2025-11-21T09:38:09Z",
		Type:        defs.LogicalBackup,
		StartTS:     time.Now().Unix(),
		LastWriteTS: lastWriteTS,
		Status:      defs.StatusDone,
		Replsets: []backup.BackupReplset{
			{
				Name:         "rs0",
				FirstWriteTS: primitive.Timestamp{T: 500, I: 0},
				LastWriteTS:  lastWriteTS,
			},
		},
	}
}

func cleanupControlColl(t *testing.T) {
	ctx := context.Background()
	_, err := connClient.BcpCollection().DeleteMany(ctx, bson.D{})
	if err != nil {
		t.Fatalf("clean up backup meta: %v", err)
	}
	_, err = connClient.PITRChunksCollection().DeleteMany(ctx, bson.D{})
	if err != nil {
		t.Fatalf("clean up chunks: %v", err)
	}
	_, err = connClient.RestoresCollection().DeleteMany(ctx, bson.D{})
	if err != nil {
		t.Fatalf("clean up restores: %v", err)
	}
	_, err = connClient.ConfigCollection().DeleteMany(ctx, bson.D{})
	if err != nil {
		t.Fatalf("clean up config: %v", err)
	}
}
