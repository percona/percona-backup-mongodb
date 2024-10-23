package snapshot

import (
	"io"
	"log"
	"runtime"
	"time"

	"github.com/mongodb/mongo-tools/common/archive"
	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/common/progress"
	"github.com/mongodb/mongo-tools/mongodump"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/version"
)

type backuper struct {
	d  *mongodump.MongoDump
	pm *progress.BarWriter
}

func NewBackup(curi string, maxParallelColls int, d, c string) (*backuper, error) {
	var err error

	opts := options.New("pbm-agent:dump", version.Current().Version, "", "", false,
		options.EnabledOptions{Auth: true, Connection: true, Namespace: true, URI: true})
	opts.URI, err = options.NewURI(curi)
	if err != nil {
		return nil, errors.Wrap(err, "parse connection string")
	}

	err = opts.NormalizeOptionsAndURI()
	if err != nil {
		return nil, errors.Wrap(err, "parse opts")
	}

	opts.Direct = true
	opts.Namespace = &options.Namespace{DB: d, Collection: c}
	if opts.Auth.IsSet() && opts.Auth.Source == "" {
		if opts.Auth.RequiresExternalDB() {
			opts.Auth.Source = "$external"
		} else {
			opts.Auth.Source = "admin"
		}
	}

	// mongodump calls runtime.GOMAXPROCS(MaxProcs).
	opts.MaxProcs = runtime.GOMAXPROCS(0)

	if maxParallelColls < 1 {
		maxParallelColls = 1
	}

	backup := &backuper{}

	backup.pm = progress.NewBarWriter(&progressWriter{}, time.Second*60, 24, false)
	backup.d = &mongodump.MongoDump{
		ToolOptions: opts,
		OutputOptions: &mongodump.OutputOptions{
			// Archive = "-" means, for mongodump, use the provided Writer
			// instead of creating a file. This is not clear at plain sight,
			// you nee to look the code to discover it.
			Archive:                "-",
			NumParallelCollections: maxParallelColls,
		},
		InputOptions:      &mongodump.InputOptions{},
		SessionProvider:   &db.SessionProvider{},
		ProgressManager:   backup.pm,
		SkipUsersAndRoles: d != "",
	}
	return backup, nil
}

// "logger" for the mongodup's ProgressManager.
// need it to be able to write new progress data in a new line
type progressWriter struct{}

func (*progressWriter) Write(m []byte) (int, error) {
	log.Printf("%s", m)
	return len(m), nil
}

// Write always return 0 as written bytes. Needed to satisfy interface
func (d *backuper) WriteTo(to io.Writer) (int64, error) {
	err := d.d.Init()
	if err != nil {
		return 0, errors.Wrap(err, "init")
	}

	d.pm.Start()
	defer d.pm.Stop()

	d.d.OutputWriter = to
	err = d.d.Dump()

	return 0, errors.Wrap(err, "make dump")
}

type DummyBackup struct{}

func (DummyBackup) WriteTo(w io.Writer) (int64, error) {
	p := archive.Prelude{
		Header: &archive.Header{
			ToolVersion: version.Current().Version,
		},
	}
	return 0, p.Write(w)
}
