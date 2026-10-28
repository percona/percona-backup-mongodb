package backup

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-backup-mongodb/x/pbm/compress"
	"github.com/percona/percona-backup-mongodb/x/pbm/defs"
	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
)

func TestOptionsValidate(t *testing.T) {
	t.Run("fills in defaults", func(t *testing.T) {
		opts := Options{Type: defs.PhysicalBackup}

		if err := opts.validate(); err != nil {
			t.Fatalf("validate: %v", err)
		}

		if opts.Type != defs.PhysicalBackup {
			t.Errorf("Type = %q, want %q", opts.Type, defs.PhysicalBackup)
		}
		if opts.Compression != defaultCompression {
			t.Errorf("Compression = %q, want %q", opts.Compression, defaultCompression)
		}
		if opts.NumParallelFiles != defaultNumParallelFiles {
			t.Errorf("NumParallelFiles = %d, want %d",
				opts.NumParallelFiles, defaultNumParallelFiles)
		}
	})

	t.Run("keeps what the client asked for", func(t *testing.T) {
		level := 3
		opts := Options{
			Type:             defs.PhysicalBackup,
			Compression:      compress.CompressionTypeZstandard,
			CompressionLevel: &level,
			NumParallelFiles: 4,
			Profile:          "remote",
		}
		want := opts

		if err := opts.validate(); err != nil {
			t.Fatalf("validate: %v", err)
		}
		if opts != want {
			t.Errorf("options = %+v, want %+v", opts, want)
		}
	})

	for name, opts := range map[string]Options{
		"empty type":              {},
		"logical type":            {Type: defs.LogicalBackup},
		"incremental type":        {Type: defs.IncrementalBackup},
		"external type":           {Type: defs.ExternalBackup},
		"unknown type":            {Type: "whatever"},
		"unknown compression":     {Type: defs.PhysicalBackup, Compression: "brotli"},
		"negative parallel files": {Type: defs.PhysicalBackup, NumParallelFiles: -1},
	} {
		t.Run(name+" is rejected", func(t *testing.T) {
			err := opts.validate()
			if !errors.Is(err, ErrInvalidOptions) {
				t.Errorf("validate: %v, want %v", err, ErrInvalidOptions)
			}
		})
	}
}

func TestResolveFirstLastWriteForCluster(t *testing.T) {
	ctx := context.Background()

	t.Run("takes the latest first and last write of the cluster", func(t *testing.T) {
		svc := &PhysSvc{repo: newTestRepo(t)}

		meta := testMeta("bcp")
		meta.Replsets = []BackupReplset{
			{
				Name:         "rs0",
				FirstWriteTS: bson.Timestamp{T: 10, I: 1},
				LastWriteTS:  bson.Timestamp{T: 30, I: 1},
			},
			{
				Name:         "rs1",
				FirstWriteTS: bson.Timestamp{T: 20, I: 4},
				LastWriteTS:  bson.Timestamp{T: 25, I: 9},
			},
			{
				// the same seconds as its peers, so the ordering has to fall
				// back on the increment
				Name:         "cfg",
				FirstWriteTS: bson.Timestamp{T: 20, I: 7},
				LastWriteTS:  bson.Timestamp{T: 30, I: 5},
			},
		}
		if err := svc.repo.Insert(ctx, meta); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		fw, lw, err := svc.resolveFirstLastWriteForCluster(ctx, "bcp")
		if err != nil {
			t.Fatalf("resolveFirstLastWriteForCluster: %v", err)
		}

		wantFW := bson.Timestamp{T: 20, I: 7}
		if fw != wantFW {
			t.Errorf("first write = %v, want %v", fw, wantFW)
		}
		wantLW := bson.Timestamp{T: 30, I: 5}
		if lw != wantLW {
			t.Errorf("last write = %v, want %v", lw, wantLW)
		}
	})

	t.Run("a single replset gives its own timestamps", func(t *testing.T) {
		svc := &PhysSvc{repo: newTestRepo(t)}

		meta := testMeta("bcp")
		meta.Replsets = []BackupReplset{
			{
				Name:         "rs0",
				FirstWriteTS: bson.Timestamp{T: 10, I: 1},
				LastWriteTS:  bson.Timestamp{T: 30, I: 2},
			},
		}
		if err := svc.repo.Insert(ctx, meta); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		fw, lw, err := svc.resolveFirstLastWriteForCluster(ctx, "bcp")
		if err != nil {
			t.Fatalf("resolveFirstLastWriteForCluster: %v", err)
		}

		if fw != (bson.Timestamp{T: 10, I: 1}) {
			t.Errorf("first write = %v, want %v", fw, bson.Timestamp{T: 10, I: 1})
		}
		if lw != (bson.Timestamp{T: 30, I: 2}) {
			t.Errorf("last write = %v, want %v", lw, bson.Timestamp{T: 30, I: 2})
		}
	})

	t.Run("reports an unknown backup", func(t *testing.T) {
		svc := &PhysSvc{repo: newTestRepo(t)}

		fw, lw, err := svc.resolveFirstLastWriteForCluster(ctx, "ghost")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("err = %v, want %v", err, ErrNotFound)
		}
		if fw != (bson.Timestamp{}) || lw != (bson.Timestamp{}) {
			t.Errorf("got %v, %v, want zero timestamps", fw, lw)
		}
	})
}
