package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/compress"
	plog "github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

const cursorCreateRetries = 10

type Meta struct {
	ID           UUID                `bson:"backupId"`
	DBpath       string              `bson:"dbpath"`
	OplogStart   BCoplogTS           `bson:"oplogStart"`
	OplogEnd     BCoplogTS           `bson:"oplogEnd"`
	CheckpointTS primitive.Timestamp `bson:"checkpointTimestamp"`
}

type BCoplogTS struct {
	TS primitive.Timestamp `bson:"ts"`
	T  int64               `bson:"t"`
}

// see https://www.percona.com/blog/2021/06/07/experimental-feature-backupcursorextend-in-percona-server-for-mongodb/
type BackupCursorData struct {
	Meta *Meta
	Data []pbm.File
}
type BackupCursor struct {
	id    UUID
	n     *pbm.Node
	l     *plog.Event
	opts  bson.D
	close chan struct{}
}

func NewBackupCursor(n *pbm.Node, l *plog.Event, opts bson.D) *BackupCursor {
	return &BackupCursor{
		n:    n,
		l:    l,
		opts: opts,
	}
}

func (bc *BackupCursor) create(ctx context.Context, retry int) (cur *mongo.Cursor, err error) {
	for i := 0; i < retry; i++ {
		cur, err = bc.n.Session().Database("admin").Aggregate(ctx, mongo.Pipeline{
			{{"$backupCursor", bc.opts}},
		})
		if err != nil {
			if se, ok := err.(mongo.ServerError); ok && se.HasErrorCode(50915) {
				// {code: 50915,name: BackupCursorOpenConflictWithCheckpoint, categories: [RetriableError]}
				// https://github.com/percona/percona-server-mongodb/blob/psmdb-6.0.6-5/src/mongo/base/error_codes.yml#L526
				bc.l.Debug("a checkpoint took place, retrying")
				time.Sleep(time.Second * time.Duration(i+1))
				continue
			}
		} else if err != nil {
			return nil, err
		}

		return cur, nil
	}

	return cur, err
}

func (bc *BackupCursor) Data(ctx context.Context) (bcp *BackupCursorData, err error) {
	cur, err := bc.create(ctx, cursorCreateRetries)
	if err != nil {
		return nil, errors.Wrap(err, "create backupCursor")
	}
	defer func() {
		if err != nil {
			cur.Close(ctx)
		}
	}()

	var m *Meta
	var files []pbm.File
	for cur.TryNext(ctx) {
		// metadata is the first
		if m == nil {
			mc := struct {
				Data Meta `bson:"metadata"`
			}{}
			err = cur.Decode(&mc)
			if err != nil {
				return nil, errors.Wrap(err, "decode metadata")
			}
			m = &mc.Data
			continue
		}

		var d pbm.File
		err = cur.Decode(&d)
		if err != nil {
			return nil, errors.Wrap(err, "decode filename")
		}

		files = append(files, d)
	}

	bc.id = m.ID

	bc.close = make(chan struct{})
	go func() {
		tk := time.NewTicker(time.Minute * 1)
		defer tk.Stop()
		for {
			select {
			case <-bc.close:
				bc.l.Debug("stop cursor polling: %v, cursor err: %v",
					cur.Close(context.Background()), cur.Err()) // `ctx` is already cancelled, so use a background context
				return
			case <-tk.C:
				cur.TryNext(ctx)
			}
		}
	}()

	return &BackupCursorData{m, files}, nil
}

func (bc *BackupCursor) Journals(upto primitive.Timestamp) ([]pbm.File, error) {
	ctx := context.Background()
	cur, err := bc.n.Session().Database("admin").Aggregate(ctx,
		mongo.Pipeline{
			{{"$backupCursorExtend", bson.D{{"backupId", bc.id}, {"timestamp", upto}}}},
		})
	if err != nil {
		return nil, errors.Wrap(err, "create backupCursorExtend")
	}
	defer cur.Close(ctx)

	var j []pbm.File

	err = cur.All(ctx, &j)
	return j, err
}

func (bc *BackupCursor) Close() {
	if bc.close != nil {
		close(bc.close)
	}
}

func (b *Backup) doPhysical(ctx context.Context, bcp *pbm.BackupCmd, opid pbm.OPID, rsMeta *pbm.BackupReplset, inf *pbm.NodeInfo, stg storage.Storage, l *plog.Event) error {
	currOpts := bson.D{}
	if b.typ == pbm.IncrementalBackup {
		currOpts = bson.D{
			{"thisBackupName", pbm.BackupCursorName(bcp.Name)},
			{"incrementalBackup", true},
		}
		if !b.incrBase {
			src, err := b.cn.LastIncrementalBackup()
			if err != nil {
				return errors.Wrap(err, "define source backup")
			}
			if src == nil {
				return errors.Wrap(err, "nil source backup")
			}

			// ? should be done during Init()?
			if inf.IsLeader() {
				err := b.cn.SetSrcBackup(bcp.Name, src.Name)
				if err != nil {
					return errors.Wrap(err, "set source backup in meta")
				}
			}
			currOpts = append(currOpts, bson.E{"srcBackupName", pbm.BackupCursorName(src.Name)})
		} else {
			// We don't need any previous incremental backup history if
			// this is a base backup. So we can flush it to free up resources.
			l.Debug("flush incremental backup history")
			cr, err := NewBackupCursor(b.node, l, bson.D{
				{"disableIncrementalBackup", true},
			}).create(ctx, cursorCreateRetries)
			if err != nil {
				l.Warning("flush incremental backup history error: %v", err)
			} else {
				err = cr.Close(ctx)
				if err != nil {
					l.Warning("close cursor disableIncrementalBackup: %v", err)
				}
			}
		}
	}
	cursor := NewBackupCursor(b.node, l, currOpts)
	defer cursor.Close()

	bcur, err := cursor.Data(ctx)
	if err != nil {
		if b.typ == pbm.IncrementalBackup && strings.Contains(err.Error(), "(UnknownError) 2: No such file or directory") {
			return errors.New("can't find incremental backup history." +
				" Previous backup was made on another node." +
				" You can make a new base incremental backup to start a new history.")
		}
		return errors.Wrap(err, "get backup files")
	}

	l.Debug("backup cursor id: %s", bcur.Meta.ID)

	lwts, err := pbm.LastWrite(b.node.Session(), true)
	if err != nil {
		return errors.Wrap(err, "get shard's last write ts")
	}

	defOpts := new(pbm.MongodOpts)
	defOpts.Storage.WiredTiger.EngineConfig.JournalCompressor = "snappy"
	defOpts.Storage.WiredTiger.CollectionConfig.BlockCompressor = "snappy"
	defOpts.Storage.WiredTiger.IndexConfig.PrefixCompression = true

	mopts, err := b.node.GetOpts(defOpts)
	if err != nil {
		return errors.Wrap(err, "get mongod options")
	}

	rsMeta.MongodOpts = mopts
	rsMeta.Status = pbm.StatusRunning
	rsMeta.FirstWriteTS = bcur.Meta.OplogEnd.TS
	rsMeta.LastWriteTS = lwts
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

		err = b.setClusterLastWrite(bcp.Name)
		if err != nil {
			return errors.Wrap(err, "set cluster last write ts")
		}
	}

	// Waiting for cluster's StatusRunning to move further.
	err = b.waitForStatus(bcp.Name, pbm.StatusRunning, nil)
	if err != nil {
		return errors.Wrap(err, "waiting for running")
	}

	_, lwTS, err := b.waitForFirstLastWrite(bcp.Name)
	if err != nil {
		return errors.Wrap(err, "get cluster first & last write ts")
	}

	l.Debug("set journal up to %v", lwTS)

	jrnls, err := cursor.Journals(lwTS)
	if err != nil {
		return errors.Wrap(err, "get journal files")
	}

	data := bcur.Data
	stgb, err := getStorageBSON(bcur.Meta.DBpath)
	if err != nil {
		return errors.Wrap(err, "check storage.bson file")
	}
	if stgb != nil {
		data = append(data, *stgb)
	}

	if b.typ == pbm.ExternalBackup {
		return b.handleExternal(bcp, rsMeta, data, jrnls, bcur.Meta.DBpath, opid, inf, l)
	}

	return b.uploadPhysical(ctx, bcp, rsMeta, data, jrnls, bcur.Meta.DBpath, stg, l)
}

func (b *Backup) handleExternal(bcp *pbm.BackupCmd, rsMeta *pbm.BackupReplset, data, jrnls []pbm.File, dbpath string, opid pbm.OPID, inf *pbm.NodeInfo, l *plog.Event) error {
	for _, f := range append(data, jrnls...) {
		f.Name = path.Clean("./" + strings.TrimPrefix(f.Name, dbpath))
		rsMeta.Files = append(rsMeta.Files, f)
	}

	// We'll rewrite rs' LastWriteTS with the cluster LastWriteTS for the meta
	// stored along with the external backup files. So do copy to preserve
	// original LastWriteTS in the meta stored on PBM storage. As rsMeta might
	// be used outside of this method.
	fsMeta := *rsMeta
	bmeta, err := b.cn.GetBackupMeta(bcp.Name)
	if err == nil {
		fsMeta.LastWriteTS = bmeta.LastWriteTS
	} else {
		l.Warning("define LastWriteTS: get backup meta: %v", err)
	}
	// save rs meta along with the data files so it can be used during the restore
	metaf := fmt.Sprintf(pbm.ExternalRsMetaFile, fsMeta.Name)
	fsMeta.Files = append(fsMeta.Files, pbm.File{
		Name: metaf,
	})
	metadst := filepath.Join(dbpath, metaf)
	err = writeRSmetaToDisk(metadst, &fsMeta)
	if err != nil {
		// we can restore without it
		l.Warning("failed to save rs meta file <%s>: %v", metadst, err)
	}

	err = b.cn.RSSetPhyFiles(bcp.Name, rsMeta.Name, rsMeta)
	if err != nil {
		return errors.Wrap(err, "set shard's files list")
	}

	err = b.toState(pbm.StatusCopyReady, bcp.Name, opid.String(), inf, nil)
	if err != nil {
		return errors.Wrapf(err, "converge to %s", pbm.StatusCopyReady)
	}

	l.Info("waiting for the datadir to be copied")
	err = b.waitForStatus(bcp.Name, pbm.StatusCopyDone, nil)
	if err != nil {
		return errors.Wrapf(err, "waiting for %s", pbm.StatusCopyDone)
	}

	err = os.Remove(metadst)
	if err != nil {
		l.Warning("remove rs meta file <%s>: %v", metadst, err)
	}

	return nil
}

func writeRSmetaToDisk(fname string, rsMeta *pbm.BackupReplset) error {
	fw, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return errors.Wrapf(err, "create/open")
	}
	defer fw.Close()

	enc := json.NewEncoder(fw)
	enc.SetIndent("", "\t")
	err = enc.Encode(rsMeta)
	if err != nil {
		return errors.Wrap(err, "write")
	}
	err = fw.Sync()
	if err != nil {
		return errors.Wrap(err, "fsync")
	}

	return nil
}

func (b *Backup) uploadPhysical(ctx context.Context, bcp *pbm.BackupCmd, rsMeta *pbm.BackupReplset, data, jrnls []pbm.File, dbpath string, stg storage.Storage, l *plog.Event) error {
	var err error
	l.Info("uploading data")
	rsMeta.Files, err = uploadFiles(ctx, data, bcp.Name+"/"+rsMeta.Name, dbpath,
		b.typ == pbm.IncrementalBackup, stg, bcp.Compression, bcp.CompressionLevel, l)
	if err != nil {
		return err
	}
	l.Info("uploading data done")

	l.Info("uploading journals")
	ju, err := uploadFiles(ctx, jrnls, bcp.Name+"/"+rsMeta.Name, dbpath,
		false, stg, bcp.Compression, bcp.CompressionLevel, l)
	if err != nil {
		return err
	}
	l.Info("uploading journals done")
	rsMeta.Files = append(rsMeta.Files, ju...)

	err = b.cn.RSSetPhyFiles(bcp.Name, rsMeta.Name, rsMeta)
	if err != nil {
		return errors.Wrap(err, "set shard's files list")
	}

	size := int64(0)
	for _, f := range rsMeta.Files {
		size += f.StgSize
	}

	err = b.cn.IncBackupSize(ctx, bcp.Name, size)
	if err != nil {
		return errors.Wrap(err, "inc backup size")
	}

	return nil
}

const storagebson = "storage.bson"

func getStorageBSON(dbpath string) (*pbm.File, error) {
	f, err := os.Stat(path.Join(dbpath, storagebson))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &pbm.File{
		Name:  path.Join(dbpath, storagebson),
		Len:   f.Size(),
		Size:  f.Size(),
		Fmode: f.Mode(),
	}, nil
}

// UUID represents a UUID as saved in MongoDB
type UUID struct{ uuid.UUID }

// MarshalBSONValue implements the bson.ValueMarshaler interface.
func (id UUID) MarshalBSONValue() (bsontype.Type, []byte, error) {
	return bsontype.Binary, bsoncore.AppendBinary(nil, 4, id.UUID[:]), nil
}

// UnmarshalBSONValue implements the bson.ValueUnmarshaler interface.
func (id *UUID) UnmarshalBSONValue(t bsontype.Type, raw []byte) error {
	if t != bsontype.Binary {
		return fmt.Errorf("invalid format on unmarshal bson value")
	}

	_, data, _, ok := bsoncore.ReadBinary(raw)
	if !ok {
		return fmt.Errorf("not enough bytes to unmarshal bson value")
	}

	copy(id.UUID[:], data)

	return nil
}

// IsZero implements the bson.Zeroer interface.
func (id *UUID) IsZero() bool {
	return bytes.Equal(id.UUID[:], uuid.Nil[:])
}

// Uploads given files to the storage. files may come as 16Mb (by default)
// blocks in that case it will concat consecutive blocks in one bigger file.
// For example: f1[0-16], f1[16-24], f1[64-16] becomes f1[0-24], f1[50-16].
// If this is an incremental, NOT base backup, it will skip uploading of
// unchanged files (Len == 0) but add them to the meta as we need know
// what files shouldn't be restored (those which isn't in the target backup).
func uploadFiles(ctx context.Context, files []pbm.File, subdir, trimPrefix string, incr bool,
	stg storage.Storage, comprT compress.CompressionType, comprL *int, l *plog.Event) (data []pbm.File, err error) {
	if len(files) == 0 {
		return data, err
	}

	trim := func(fname string) string {
		// path.Clean to get rid of `/` at the beginning in case it's
		// left after TrimPrefix. Just for consistent file names in metadata
		return path.Clean("./" + strings.TrimPrefix(fname, trimPrefix))
	}

	wfile := files[0]
	for _, file := range files[1:] {
		select {
		case <-ctx.Done():
			return nil, ErrCancelled
		default:
		}

		// Skip uploading unchanged files if incremental
		// but add them to the meta to keep track of files to be restored
		// from prev backups. Plus sometimes the cursor can return an offset
		// beyond the current file size. Such phantom changes shouldn't
		// be copied. But save meta to have file size.
		if incr && (file.Len == 0 || file.Off >= file.Size) {
			file.Off = -1
			file.Len = -1
			file.Name = trim(file.Name)

			data = append(data, file)
			continue
		}

		if wfile.Name == file.Name &&
			wfile.Off+wfile.Len == file.Off {
			wfile.Len += file.Len
			wfile.Size = file.Size
			continue
		}

		fw, err := writeFile(ctx, wfile, path.Join(subdir, trim(wfile.Name)), stg, comprT, comprL, l)
		if err != nil {
			return data, errors.Wrapf(err, "upload file `%s`", wfile.Name)
		}
		fw.Name = trim(wfile.Name)

		data = append(data, *fw)

		wfile = file
	}

	if incr && wfile.Off == 0 && wfile.Len == 0 {
		return data, nil
	}

	f, err := writeFile(ctx, wfile, path.Join(subdir, trim(wfile.Name)), stg, comprT, comprL, l)
	if err != nil {
		return data, errors.Wrapf(err, "upload file `%s`", wfile.Name)
	}
	f.Name = trim(wfile.Name)

	data = append(data, *f)

	return data, nil
}

func writeFile(ctx context.Context, src pbm.File, dst string, stg storage.Storage, compression compress.CompressionType, compressLevel *int, l *plog.Event) (*pbm.File, error) {
	fstat, err := os.Stat(src.Name)
	if err != nil {
		return nil, errors.Wrap(err, "get file stat")
	}

	dst += compression.Suffix()
	sz := fstat.Size()
	if src.Len != 0 {
		// Len is always a multiple of the fixed size block (16Mb default)
		// so Off + Len might be bigger than the actual file size
		sz = src.Len
		if src.Off+src.Len > src.Size {
			sz = src.Size - src.Off
		}
		dst += fmt.Sprintf(".%d-%d", src.Off, src.Len)
	}
	l.Debug("uploading: %s %s", src, fmtSize(sz))

	_, err = Upload(ctx, &src, stg, compression, compressLevel, dst, sz)
	if err != nil {
		return nil, errors.Wrap(err, "upload file")
	}

	finf, err := stg.FileStat(dst)
	if err != nil {
		return nil, errors.Wrapf(err, "get storage file stat %s", dst)
	}

	return &pbm.File{
		Name:    src.Name,
		Size:    fstat.Size(),
		Fmode:   fstat.Mode(),
		StgSize: finf.Size,
		Off:     src.Off,
		Len:     src.Len,
	}, nil
}

func fmtSize(size int64) string {
	const (
		_          = iota
		KB float64 = 1 << (10 * iota)
		MB
		GB
		TB
	)

	s := float64(size)

	switch {
	case s >= TB:
		return fmt.Sprintf("%.2fTB", s/TB)
	case s >= GB:
		return fmt.Sprintf("%.2fGB", s/GB)
	case s >= MB:
		return fmt.Sprintf("%.2fMB", s/MB)
	case s >= KB:
		return fmt.Sprintf("%.2fKB", s/KB)
	}
	return fmt.Sprintf("%.2fB", s)
}
