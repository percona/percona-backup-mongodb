package backup

import (
	"context"
	"io"

	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/archive"
	plog "github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/snapshot"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

func (b *Backup) doLogical(ctx context.Context, bcp *pbm.BackupCmd, opid pbm.OPID, rsMeta *pbm.BackupReplset, inf *pbm.NodeInfo, stg storage.Storage, l *plog.Event) error {
	oplog := oplog.NewOplogBackup(b.node)
	oplogTS, err := oplog.LastWrite()
	if err != nil {
		return errors.Wrap(err, "define oplog start position")
	}

	rsMeta.Status = pbm.StatusRunning
	rsMeta.FirstWriteTS = oplogTS
	rsMeta.DumpName = archive.FormatFilepath(bcp.Name, rsMeta.Name, archive.MetaFile)
	rsMeta.OplogName = archive.FormatFilepath(bcp.Name, rsMeta.Name, "oplog.bson") + bcp.Compression.Suffix()
	err = b.cn.AddRSMeta(bcp.Name, *rsMeta)
	if err != nil {
		return errors.Wrap(err, "add shard's metadata")
	}

	if inf.IsLeader() {
		err := b.reconcileStatus(bcp.Name, opid.String(), pbm.StatusRunning, &pbm.WaitBackupStart)
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

	dump, err := snapshot.NewBackup(b.node.ConnURI(), b.node.DumpConns(), bcp.Namespaces)
	if err != nil {
		return errors.Wrap(err, "init mongodump options")
	}

	snapshotSize, err := archive.UploadDump(dump,
		func(filename string, r io.Reader) error {
			filepath := archive.FormatFilepath(bcp.Name, rsMeta.Name, filename)
			return stg.Save(filepath, r, 0)
		},
		archive.UploadDumpOptions{
			Compression:      bcp.Compression,
			CompressionLevel: bcp.CompressionLevel,
		})
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
		err := b.reconcileStatus(bcp.Name, opid.String(), pbm.StatusDumpDone, nil)
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
	oplogSize, err := Upload(ctx, oplog, stg, bcp.Compression, bcp.CompressionLevel, rsMeta.OplogName, -1)
	if err != nil {
		return errors.Wrap(err, "oplog")
	}

	err = b.cn.IncBackupSize(b.cn.Context(), bcp.Name, rsMeta.Name, snapshotSize+oplogSize)
	if err != nil {
		return errors.Wrap(err, "inc backup size")
	}

	return nil
}
