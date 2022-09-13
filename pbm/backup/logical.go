package backup

import (
	"context"
	"io"
	"path"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/archive"
	"github.com/percona/percona-backup-mongodb/pbm/compress"
	plog "github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/snapshot"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

func (b *Backup) doLogical(ctx context.Context, bcp *pbm.BackupCmd, opid pbm.OPID, rsMeta *pbm.BackupReplset, inf *pbm.NodeInfo, stg storage.Storage, l *plog.Event) error {
	var db, coll string
	if len(bcp.Namespaces) != 0 {
		db, coll = parseNS(bcp.Namespaces[0])
	}

	nssSize, err := getNamespacesSize(ctx, b.node.Session(), db, coll)
	if err != nil {
		return errors.WithMessage(err, "get namespaces size")
	}
	if bcp.Compression == compress.CompressionTypeNone {
		for n := range nssSize {
			nssSize[n] *= 4
		}
	}

	oplog := oplog.NewOplogBackup(b.node)
	oplogTS, err := oplog.LastWrite()
	if err != nil {
		return errors.Wrap(err, "define oplog start position")
	}

	rsMeta.Status = pbm.StatusRunning
	rsMeta.FirstWriteTS = oplogTS
	rsMeta.OplogName = path.Join(bcp.Name, rsMeta.Name, "local.oplog.rs.bson") + bcp.Compression.Suffix()
	rsMeta.DumpName = path.Join(bcp.Name, rsMeta.Name, archive.MetaFile)
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

	if len(bcp.Namespaces) == 0 {
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
	}

	var dump io.WriterTo
	if len(nssSize) == 0 {
		dump = snapshot.DummyBackup{}
	} else {
		dump, err = snapshot.NewBackup(b.node.ConnURI(), snapshot.BackupOptions{
			Concurrency: b.node.DumpConns(),
			Database:    db,
			Collection:  coll,
			NSExclude:   bcp.Exclude,
		})
		if err != nil {
			return errors.Wrap(err, "init mongodump options")
		}
	}

	cfg, err := b.cn.GetConfig()
	if err != nil {
		return errors.WithMessage(err, "get config")
	}

	snapshotSize, err := snapshot.UploadDump(dump,
		func(ns, ext string, r io.Reader) error {
			stg, err := pbm.Storage(cfg, l)
			if err != nil {
				return errors.WithMessage(err, "get storage")
			}

			filepath := path.Join(bcp.Name, rsMeta.Name, ns+ext)
			return stg.Save(filepath, r, nssSize[ns])
		},
		snapshot.UploadDumpOptions{
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

	err = b.cn.IncBackupSize(ctx, bcp.Name, snapshotSize+oplogSize)
	if err != nil {
		return errors.Wrap(err, "inc backup size")
	}

	return nil
}

func getNamespacesSize(ctx context.Context, m *mongo.Client, db, coll string) (map[string]int64, error) {
	rv := make(map[string]int64)

	q := bson.D{}
	if db != "" {
		q = append(q, bson.E{"name", db})
	}
	dbs, err := m.ListDatabaseNames(ctx, q)
	if err != nil {
		return nil, errors.WithMessage(err, "list databases")
	}
	if len(dbs) == 0 {
		return rv, nil
	}

	mu := sync.Mutex{}
	eg, ctx := errgroup.WithContext(ctx)

	for _, db := range dbs {
		db := db

		eg.Go(func() error {
			q := bson.D{}
			if coll != "" {
				q = append(q, bson.E{"name", coll})
			}
			res, err := m.Database(db).ListCollectionSpecifications(ctx, q)
			if err != nil {
				return errors.WithMessagef(err, "list collections for %q", db)
			}
			if len(res) == 0 {
				return nil
			}

			eg, ctx := errgroup.WithContext(ctx)

			for _, coll := range res {
				coll := coll

				if coll.Type == "view" || strings.HasPrefix(coll.Name, "system.buckets.") {
					continue
				}

				eg.Go(func() error {
					ns := db + "." + coll.Name
					res := m.Database(db).RunCommand(ctx, bson.D{{"collStats", coll.Name}})
					if err := res.Err(); err != nil {
						return errors.WithMessagef(err, "collStats %q", ns)
					}

					var doc struct {
						StorageSize int64 `bson:"storageSize"`
					}

					if err := res.Decode(&doc); err != nil {
						return errors.WithMessagef(err, "decode %q", ns)
					}

					mu.Lock()
					rv[ns] = doc.StorageSize
					mu.Unlock()

					return nil
				})
			}

			return eg.Wait()
		})
	}

	err = eg.Wait()
	return rv, err
}

func parseNS(ns string) (string, string) {
	var db, coll string

	switch s := strings.SplitN(ns, ".", 2); len(s) {
	case 0:
		return "", ""
	case 1:
		db = s[0]
	case 2:
		db, coll = s[0], s[1]
	}

	if db == "*" {
		db = ""
	}
	if coll == "*" {
		coll = ""
	}

	return db, coll
}
