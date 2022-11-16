package pbm

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/pbm/archive"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/version"
)

const (
	StorInitFile    = ".pbm.init"
	PhysRestoresDir = ".pbm.restore"
)

// ResyncStorage updates PBM metadata (snapshots and pitr) according to the data in the storage
func (p *PBM) ResyncStorage(l *log.Event) error {
	stg, err := p.GetStorage(l)
	if err != nil {
		return errors.Wrap(err, "unable to get backup store")
	}

	_, err = stg.FileStat(StorInitFile)
	if errors.Is(err, storage.ErrNotExist) {
		err = stg.Save(StorInitFile, bytes.NewBufferString(version.DefaultInfo.Version), 0)
	}
	if err != nil {
		return errors.Wrap(err, "init storage")
	}

	rstrs, err := stg.List(PhysRestoresDir, ".json")
	if err != nil {
		return errors.Wrap(err, "get physical restores list from the storage")
	}
	l.Debug("got physical restores list: %v", len(rstrs))
	for _, rs := range rstrs {
		rname := strings.TrimSuffix(rs.Name, ".json")
		rmeta, err := GetPhysRestoreMeta(rname, stg)
		if err != nil {
			l.Error("get meta for restore %s: %v", rs.Name, err)
			if rmeta == nil {
				continue
			}
		}

		_, err = p.Conn.Database(DB).Collection(RestoresCollection).ReplaceOne(
			p.ctx,
			bson.D{{"name", rmeta.Name}},
			rmeta,
			options.Replace().SetUpsert(true),
		)
		if err != nil {
			return errors.Wrapf(err, "upsert restore %s/%s", rmeta.Name, rmeta.Backup)
		}
	}

	bcps, err := stg.List("", MetadataFileSuffix)
	if err != nil {
		return errors.Wrap(err, "get a backups list from the storage")
	}
	l.Debug("got backups list: %v", len(bcps))

	err = p.moveCollection(BcpCollection, BcpOldCollection)
	if err != nil {
		return errors.Wrapf(err, "copy current backups meta from %s to %s", BcpCollection, BcpOldCollection)
	}
	err = p.moveCollection(PITRChunksCollection, PITRChunksOldCollection)
	if err != nil {
		return errors.Wrapf(err, "copy current pitr meta from %s to %s", PITRChunksCollection, PITRChunksOldCollection)
	}

	var ins []interface{}
	for _, b := range bcps {
		l.Debug("bcp: %v", b.Name)

		d, err := stg.SourceReader(b.Name)
		if err != nil {
			return errors.Wrapf(err, "read meta for %v", b.Name)
		}

		v := BackupMeta{}
		err = json.NewDecoder(d).Decode(&v)
		d.Close()
		if err != nil {
			return errors.Wrapf(err, "unmarshal backup meta [%s]", b.Name)
		}
		err = checkBackupFiles(p.ctx, &v, stg)
		if err != nil {
			l.Warning("skip snapshot %s: %v", v.Name, err)
			v.Status = StatusError
			v.Err = err.Error()
		}
		ins = append(ins, v)
	}

	if len(ins) != 0 {
		_, err = p.Conn.Database(DB).Collection(BcpCollection).InsertMany(p.ctx, ins)
		if err != nil {
			return errors.Wrap(err, "insert retrieved backups meta")
		}
	}

	pitrf, err := stg.List(PITRfsPrefix, "")
	if err != nil {
		return errors.Wrap(err, "get list of pitr chunks")
	}
	if len(pitrf) == 0 {
		return nil
	}

	var pitr []interface{}
	for _, f := range pitrf {
		_, err := stg.FileStat(PITRfsPrefix + "/" + f.Name)
		if err != nil {
			l.Warning("skip pitr chunk %s/%s because of %v", PITRfsPrefix, f.Name, err)
			continue
		}
		chnk := PITRmetaFromFName(f.Name)
		if chnk != nil {
			pitr = append(pitr, chnk)
		}
	}

	if len(pitr) == 0 {
		return nil
	}

	_, err = p.Conn.Database(DB).Collection(PITRChunksCollection).InsertMany(p.ctx, pitr)
	if err != nil {
		return errors.Wrap(err, "insert retrieved pitr meta")
	}

	return nil
}

func checkBackupFiles(ctx context.Context, bcp *BackupMeta, stg storage.Storage) error {
	// !!! TODO: Check physical files ?
	if bcp.Type == PhysicalBackup || bcp.Type == IncrementalBackup {
		return nil
	}

	legacy := version.IsLegacyArchive(bcp.PBMVersion)
	eg, _ := errgroup.WithContext(ctx)
	for _, rs := range bcp.Replsets {
		rs := rs

		eg.Go(func() error { return checkFile(stg, rs.DumpName) })
		eg.Go(func() error { return checkFile(stg, rs.OplogName) })

		if legacy {
			continue
		}

		nss, err := ReadArchiveNamespaces(stg, rs.DumpName)
		if err != nil {
			return errors.WithMessagef(err, "parse metafile %q", rs.DumpName)
		}

		for _, ns := range nss {
			if ns.Size == 0 {
				continue
			}

			ns := archive.NSify(ns.Database, ns.Collection)
			f := path.Join(bcp.Name, rs.Name, ns+bcp.Compression.Suffix())

			eg.Go(func() error { return checkFile(stg, f) })
		}
	}

	return eg.Wait()
}

func ReadArchiveNamespaces(stg storage.Storage, metafile string) ([]*archive.Namespace, error) {
	r, err := stg.SourceReader(metafile)
	if err != nil {
		return nil, errors.WithMessagef(err, "open %q", metafile)
	}
	defer r.Close()

	meta, err := archive.ReadMetadata(r)
	if err != nil {
		return nil, errors.WithMessagef(err, "parse metafile %q", metafile)
	}

	return meta.Namespaces, nil
}

func checkFile(stg storage.Storage, filename string) error {
	f, err := stg.FileStat(filename)
	if err != nil {
		return errors.WithMessagef(err, "file %q", filename)
	}
	if f.Size == 0 {
		return errors.Errorf("%q is empty", filename)
	}

	return nil
}

func (p *PBM) moveCollection(coll, as string) error {
	err := p.Conn.Database(DB).Collection(as).Drop(p.ctx)
	if err != nil {
		return errors.Wrap(err, "failed to remove old archive from backups metadata")
	}

	cur, err := p.Conn.Database(DB).Collection(coll).Find(p.ctx, bson.M{})
	if err != nil {
		return errors.Wrap(err, "get current data")
	}
	for cur.Next(p.ctx) {
		_, err = p.Conn.Database(DB).Collection(as).InsertOne(p.ctx, cur.Current)
		if err != nil {
			return errors.Wrapf(err, "insert")
		}
	}

	if cur.Err() != nil {
		return cur.Err()
	}

	_, err = p.Conn.Database(DB).Collection(coll).DeleteMany(p.ctx, bson.M{})
	return errors.Wrap(err, "remove current data")
}

func GetPhysRestoreMeta(restore string, stg storage.Storage) (rmeta *RestoreMeta, err error) {
	mjson := filepath.Join(PhysRestoresDir, restore) + ".json"
	_, err = stg.FileStat(mjson)
	if err != nil && err != storage.ErrNotExist {
		return nil, errors.Wrapf(err, "get file %s", mjson)
	}
	if err == nil {
		src, err := stg.SourceReader(mjson)
		if err != nil {
			return nil, errors.Wrapf(err, "get file %s", mjson)
		}

		rmeta = new(RestoreMeta)
		err = json.NewDecoder(src).Decode(rmeta)
		if err != nil {
			return nil, errors.Wrapf(err, "decode meta %s", mjson)
		}
	}

	condsm, err := ParsePhysRestoreStatus(restore, stg)
	if err != nil {
		return rmeta, errors.Wrap(err, "parse physical restore status")
	}

	if rmeta == nil {
		return condsm, err
	}

	rmeta.Replsets = condsm.Replsets
	if condsm.Status != "" {
		rmeta.Status = condsm.Status
	}
	rmeta.LastTransitionTS = condsm.LastTransitionTS
	if condsm.Error != "" {
		rmeta.Error = condsm.Error
	}
	rmeta.Hb = condsm.Hb
	rmeta.Conditions = condsm.Conditions
	rmeta.Type = PhysicalBackup

	return rmeta, err
}

// ParsePhysRestoreStatus parses phys restore's sync files and creates RestoreMeta.
//
// On files format, see comments for *PhysRestore.toState() in pbm/restore/physical.go
func ParsePhysRestoreStatus(restore string, stg storage.Storage) (*RestoreMeta, error) {
	rfiles, err := stg.List(PhysRestoresDir+"/"+restore, "")
	if err != nil {
		return nil, errors.Wrap(err, "get files")
	}

	meta := RestoreMeta{Name: restore, Type: PhysicalBackup}

	rss := make(map[string]struct {
		rs    RestoreReplset
		nodes map[string]RestoreNode
	})

	for _, f := range rfiles {
		parts := strings.SplitN(f.Name, ".", 2)
		if len(parts) != 2 {
			continue
		}
		switch parts[0] {
		case "rs":
			rsparts := strings.Split(parts[1], "/")

			if len(rsparts) < 2 {
				continue
			}

			rsName := strings.TrimPrefix(rsparts[0], "rs.")
			rs, ok := rss[rsName]
			if !ok {
				rs.rs.Name = rsName
				rs.nodes = make(map[string]RestoreNode)
			}

			p := strings.Split(rsparts[1], ".")

			if len(p) < 2 {
				continue
			}
			switch p[0] {
			case "node":
				if len(p) < 3 {
					continue
				}
				nName := strings.Join(p[1:len(p)-1], ".")
				node, ok := rs.nodes[nName]
				if !ok {
					node.Name = nName
				}
				cond, err := parsePhysRestoreCond(stg, f.Name, restore)
				if err != nil {
					return nil, err
				}
				if cond.Status == "hb" {
					node.Hb.T = uint32(cond.Timestamp)
				} else {
					node.Conditions.Insert(cond)
					l := node.Conditions[len(node.Conditions)-1]
					node.Status = l.Status
					node.LastTransitionTS = l.Timestamp
					node.Error = l.Error
				}

				rs.nodes[nName] = node
			case "rs":
				cond, err := parsePhysRestoreCond(stg, f.Name, restore)
				if err != nil {
					return nil, err
				}
				if cond.Status == "hb" {
					rs.rs.Hb.T = uint32(cond.Timestamp)
				} else {
					rs.rs.Conditions.Insert(cond)
					l := rs.rs.Conditions[len(rs.rs.Conditions)-1]
					rs.rs.Status = l.Status
					rs.rs.LastTransitionTS = l.Timestamp
					rs.rs.Error = l.Error
				}
			}
			rss[rsName] = rs

		case "cluster":
			cond, err := parsePhysRestoreCond(stg, f.Name, restore)
			if err != nil {
				return nil, err
			}

			if cond.Status == "hb" {
				meta.Hb.T = uint32(cond.Timestamp)
			} else {
				meta.Conditions.Insert(cond)
				lstat := meta.Conditions[len(meta.Conditions)-1]
				meta.Status = lstat.Status
				meta.LastTransitionTS = lstat.Timestamp
				meta.Error = lstat.Error
			}
		}
	}

	for _, rs := range rss {
		for _, node := range rs.nodes {
			rs.rs.Nodes = append(rs.rs.Nodes, node)
		}
		meta.Replsets = append(meta.Replsets, rs.rs)
	}

	return &meta, nil
}

func parsePhysRestoreCond(stg storage.Storage, fname, restore string) (*Condition, error) {
	s := strings.Split(fname, ".")
	cond := Condition{Status: Status(s[len(s)-1])}

	src, err := stg.SourceReader(filepath.Join(PhysRestoresDir, restore, fname))
	if err != nil {
		return nil, errors.Wrapf(err, "get file %s", fname)
	}
	b, err := io.ReadAll(src)
	if err != nil {
		return nil, errors.Wrapf(err, "read file %s", fname)
	}

	if cond.Status == StatusError {
		estr := strings.SplitN(string(b), ":", 2)
		if len(estr) != 2 {
			return nil, errors.Errorf("malformatted data in %s: %s", fname, b)
		}
		cond.Timestamp, err = strconv.ParseInt(estr[0], 10, 0)
		if err != nil {
			return nil, errors.Wrapf(err, "read ts from %s", fname)
		}
		cond.Error = estr[1]
		return &cond, nil
	}

	cond.Timestamp, err = strconv.ParseInt(string(b), 10, 0)
	if err != nil {
		return nil, errors.Wrapf(err, "read ts from %s", fname)
	}

	return &cond, nil
}
