package backup

import (
	"testing"

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
