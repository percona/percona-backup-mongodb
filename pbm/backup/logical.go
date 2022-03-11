package backup

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/common/progress"
	"github.com/mongodb/mongo-tools/mongodump"
	"github.com/percona/percona-backup-mongodb/pbm"
	plog "github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/pkg/errors"
)

func (b *Backup) doLogical(ctx context.Context, bcp pbm.BackupCmd, opid pbm.OPID, rsMeta *pbm.BackupReplset, inf *pbm.NodeInfo, stg storage.Storage, l *plog.Event) error {
	oplog := NewOplog(b.node)
	oplogTS, err := oplog.LastWrite()
	if err != nil {
		return errors.Wrap(err, "define oplog start position")
	}

	rsMeta.Status = pbm.StatusRunning
	rsMeta.FirstWriteTS = oplogTS
	rsMeta.OplogName = getDstName("oplog", bcp, inf.SetName)
	rsMeta.DumpName = getDstName("dump", bcp, inf.SetName)
	err = b.cn.AddRSMeta(bcp.Name, *rsMeta)
	if err != nil {
		return errors.Wrap(err, "add shard's metadata")
	}

	if inf.IsLeader() {
		err := b.reconcileStatus(bcp.Name, opid.String(), pbm.StatusRunning, inf, &pbm.WaitBackupStart)
		if err != nil {
			if errors.Cause(err) == errConvergeTimeOut {
				return errors.Wrap(err, "couldn't get response from all shards")
			}
			return errors.Wrap(err, "check cluster for backup started")
		}

		err = b.setClusterFirstWrite(bcp.Name)
		if err != nil {
			return errors.Wrap(err, "set cluster first write ts")
		}
	}

	// Waiting for cluster's StatusRunning to move further.
	err = b.waitForStatus(bcp.Name, pbm.StatusRunning, nil)
	if err != nil {
		return errors.Wrap(err, "waiting for running")
	}

	// Save users and roles to the tmp collections so the restore would copy that data
	// to the system collections. Have to do this because of issues with the restore and preserverUUID.
	// see: https://jira.percona.com/browse/PBM-636 and comments
	lw, err := b.node.CopyUsersNRolles()
	if err != nil {
		return errors.Wrap(err, "copy users and roles for the restore")
	}

	defer func() {
		l.Info("dropping tmp collections")
		err := b.node.DropTMPcoll()
		if err != nil {
			l.Warning("drop tmp users and roles: %v", err)
		}
	}()

	// before proceeding any further we have to be sure that tmp users and roles
	// have replicated to the node we're about to take a backup from
	// *copying made on a primary but backup does a secondary node
	l.Debug("wait for tmp users %v", lw)
	err = b.node.WaitForWrite(lw)
	if err != nil {
		return errors.Wrap(err, "wait for tmp users and roles replication")
	}

	sz, err := b.node.SizeDBs()
	if err != nil {
		return errors.Wrap(err, "mongodump")
	}
	// if backup wouldn't be compressed we're assuming
	// that the dump size could be up to 4x lagrer due to
	// mongo's wieredtiger compression
	if bcp.Compression == pbm.CompressionTypeNone {
		sz *= 4
	}

	dump, err := newDump(b.node.ConnURI(), b.node.DumpConns())
	if err != nil {
		return errors.Wrap(err, "init mongodump options")
	}
	_, err = Upload(ctx, dump, stg, bcp.Compression, rsMeta.DumpName, sz)
	if err != nil {
		return errors.Wrap(err, "mongodump")
	}
	l.Info("mongodump finished, waiting for the oplog")

	err = b.cn.ChangeRSState(bcp.Name, rsMeta.Name, pbm.StatusDumpDone, "")
	if err != nil {
		return errors.Wrap(err, "set shard's StatusDumpDone")
	}

	lwts, err := oplog.LastWrite()
	if err != nil {
		return errors.Wrap(err, "get shard's last write ts")
	}

	err = b.cn.SetRSLastWrite(bcp.Name, rsMeta.Name, lwts)
	if err != nil {
		return errors.Wrap(err, "set shard's last write ts")
	}

	if inf.IsLeader() {
		err := b.reconcileStatus(bcp.Name, opid.String(), pbm.StatusDumpDone, inf, nil)
		if err != nil {
			return errors.Wrap(err, "check cluster for dump done")
		}

		err = b.setClusterLastWrite(bcp.Name)
		if err != nil {
			return errors.Wrap(err, "set cluster last write ts")
		}
	}

	err = b.waitForStatus(bcp.Name, pbm.StatusDumpDone, nil)
	if err != nil {
		return errors.Wrap(err, "waiting for dump done")
	}

	fwTS, lwTS, err := b.waitForFirstLastWrite(bcp.Name)
	if err != nil {
		return errors.Wrap(err, "get cluster first & last write ts")
	}

	l.Debug("set oplog span to %v / %v", fwTS, lwTS)
	oplog.SetTailingSpan(fwTS, lwTS)
	// size -1 - we're assuming oplog never exceed 97Gb (see comments in s3.Save method)
	_, err = Upload(ctx, oplog, stg, bcp.Compression, rsMeta.OplogName, -1)
	if err != nil {
		return errors.Wrap(err, "oplog")
	}

	return nil
}

type mdump struct {
	opts  *options.ToolOptions
	conns int
}

func newDump(curi string, conns int) (*mdump, error) {
	if conns <= 0 {
		conns = 1
	}

	var err error

	opts := options.New("mongodump", "0.0.1", "none", "", true, options.EnabledOptions{Auth: true, Connection: true, Namespace: true, URI: true})
	opts.URI, err = options.NewURI(curi)
	if err != nil {
		return nil, errors.Wrap(err, "parse connection string")
	}

	err = opts.NormalizeOptionsAndURI()
	if err != nil {
		return nil, errors.Wrap(err, "parse opts")
	}

	opts.Direct = true

	return &mdump{
		opts:  opts,
		conns: conns,
	}, nil
}

// "logger" for the mongodup's ProgressManager.
// need it to be able to write new progress data in a new line
type progressWriter struct{}

func (*progressWriter) Write(m []byte) (int, error) {
	log.Printf("%s", m)
	return len(m), nil
}

// Write always return 0 as written bytes. Needed to satisfy interface
func (d *mdump) WriteTo(w io.Writer) (int64, error) {
	pm := progress.NewBarWriter(&progressWriter{}, time.Second*60, 24, false)
	mdump := mongodump.MongoDump{
		ToolOptions: d.opts,
		OutputOptions: &mongodump.OutputOptions{
			// Archive = "-" means, for mongodump, use the provided Writer
			// instead of creating a file. This is not clear at plain sight,
			// you nee to look the code to discover it.
			Archive:                "-",
			NumParallelCollections: d.conns,
		},
		InputOptions:    &mongodump.InputOptions{},
		SessionProvider: &db.SessionProvider{},
		OutputWriter:    w,
		ProgressManager: pm,
	}
	err := mdump.Init()
	if err != nil {
		return 0, errors.Wrap(err, "init")
	}
	pm.Start()
	defer pm.Stop()

	err = mdump.Dump()

	return 0, errors.Wrap(err, "make dump")
}

func getDstName(typ string, bcp pbm.BackupCmd, rsName string) string {
	name := bcp.Name

	if rsName != "" {
		name += "_" + rsName
	}

	return name + "." + typ + bcp.Compression.Suffix()
}
