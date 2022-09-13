package snapshot

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/mongodb/mongo-tools/common/archive"
	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/common/progress"
	"github.com/mongodb/mongo-tools/mongodump"
	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/version"
)

type backuper struct {
	d     *mongodump.MongoDump
	pm    *progress.BarWriter
	stopC chan struct{}
}

type BackupOptions struct {
	Concurrency int
	Database    string
	Collection  string
	NSExclude   []string
}

func NewBackup(curi string, o BackupOptions) (io.WriterTo, error) {
	if o.Concurrency <= 0 {
		o.Concurrency = 1
	}

	nsExclude := make([]string, 0, len(ExcludeFromRestore)+len(o.NSExclude))
	copy(nsExclude, ExcludeFromRestore)
	copy(nsExclude[len(ExcludeFromRestore):], o.NSExclude)

	var err error

	opts := options.New("pbm-agent:dump", version.DefaultInfo.Version, "", "", false,
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
	opts.Namespace = &options.Namespace{DB: o.Database, Collection: o.Collection}

	backup := &backuper{}
	backup.pm = progress.NewBarWriter(&progressWriter{}, time.Second*60, 24, false)
	backup.d = &mongodump.MongoDump{
		ToolOptions: opts,
		OutputOptions: &mongodump.OutputOptions{
			// Archive = "-" means, for mongodump, use the provided Writer
			// instead of creating a file. This is not clear at plain sight,
			// you nee to look the code to discover it.
			Archive:                "-",
			NumParallelCollections: o.Concurrency,
			ExcludedCollections:    nsExclude,
		},
		InputOptions:      &mongodump.InputOptions{},
		SessionProvider:   &db.SessionProvider{},
		ProgressManager:   backup.pm,
		SkipUsersAndRoles: o.Database != "" || len(o.NSExclude) != 0,
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

	d.stopC = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-ctx.Done():
		case <-d.stopC:
			d.d.HandleInterrupt()
		}

		d.stopC = nil
	}()

	d.d.OutputWriter = to
	err = d.d.Dump()

	return 0, errors.Wrap(err, "make dump")
}

func (d *backuper) Cancel() {
	if c := d.stopC; c != nil {
		select {
		case _, ok := <-c:
			if ok {
				close(c)
			}
		default:
		}
	}
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
