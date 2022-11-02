package backup

import (
	"bytes"
	"context"
	"fmt"
	"os"
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
		if err != nil && strings.Contains(err.Error(), "(Location50915)") {
			bc.l.Debug("a checkpoint took place, retrying")
			time.Sleep(time.Second * time.Duration(i+1))
			continue
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
				return errors.Wrap(err, "define base backup")
			}
			if src == nil {
				return errors.Wrap(err, "nil base backup")
			}

			// ? should be done during Init()?
			if inf.IsLeader() {
				err := b.cn.SetSrcBackup(bcp.Name, src.Name)
				if err != nil {
					return errors.Wrap(err, "set source backup in meta")
				}
			}
			currOpts = append(currOpts, bson.E{"srcBackupName", pbm.BackupCursorName(src.Name)})
		}
	}
	cursor := NewBackupCursor(b.node, l, currOpts)
	defer cursor.Close()

	bcur, err := cursor.Data(ctx)
	if err != nil {
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

	l.Info("uploading data")
	rsMeta.Journal, rsMeta.Files, err = uploadFiles(ctx, bcur.Data, bcp.Name+"/"+rsMeta.Name, bcur.Meta.DBpath+"/",
		b.typ == pbm.IncrementalBackup, stg, bcp.Compression, bcp.CompressionLevel, l)
	if err != nil {
		return err
	}
	l.Info("uploading data done")

	l.Info("uploading journals")
	ju, _, err := uploadFiles(ctx, jrnls, bcp.Name+"/"+rsMeta.Name, bcur.Meta.DBpath+"/",
		false, stg, bcp.Compression, bcp.CompressionLevel, l)
	if err != nil {
		return err
	}
	rsMeta.Journal = append(rsMeta.Journal, ju...)
	l.Info("uploading journals")

	err = b.cn.RSSetPhyFiles(bcp.Name, rsMeta.Name, rsMeta.Files)
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

const journalPrefix = "journal/WiredTigerLog."

func uploadFiles(ctx context.Context, files []pbm.File, subdir, trimPrefix string, incr bool,
	stg storage.Storage, comprT compress.CompressionType, comprL *int, l *plog.Event) (journal, data []pbm.File, err error) {
	if len(files) == 0 {
		return journal, data, err
	}

	l.Debug("$BC")
	for _, bd := range files {
		l.Debug("==> %s", bd)
	}

	wfile := files[0]
	for _, bd := range files[1:] {
		select {
		case <-ctx.Done():
			return nil, nil, ErrCancelled
		default:
		}

		if incr && bd.Off == 0 && bd.Len == 0 {
			continue
		}

		if wfile.Name == bd.Name &&
			wfile.Off+wfile.Len == bd.Off {
			wfile.Len += bd.Len
			continue
		}

		f, err := writeFile(ctx, wfile, subdir+"/"+strings.TrimPrefix(wfile.Name, trimPrefix), stg, comprT, comprL, l)
		if err != nil {
			return journal, data, errors.Wrapf(err, "upload file `%s`", wfile.Name)
		}
		f.Name = strings.TrimPrefix(wfile.Name, trimPrefix)

		if strings.HasPrefix(f.Name, journalPrefix) {
			journal = append(journal, *f)
		} else {
			data = append(data, *f)
		}

		wfile = bd
	}

	if incr && wfile.Off == 0 && wfile.Len == 0 {
		return journal, data, nil
	}

	f, err := writeFile(ctx, wfile, subdir+"/"+strings.TrimPrefix(wfile.Name, trimPrefix), stg, comprT, comprL, l)
	if err != nil {
		return journal, data, errors.Wrapf(err, "upload file `%s`", wfile.Name)
	}
	f.Name = strings.TrimPrefix(wfile.Name, trimPrefix)
	if strings.HasPrefix(f.Name, journalPrefix) {
		journal = append(journal, *f)
	} else {
		data = append(data, *f)
	}

	return journal, data, nil
}

func writeFile(ctx context.Context, src pbm.File, dst string, stg storage.Storage, compression compress.CompressionType, compressLevel *int, l *plog.Event) (*pbm.File, error) {
	fstat, err := os.Stat(src.Name)
	if err != nil {
		return nil, errors.Wrap(err, "get file stat")
	}

	dst += compression.Suffix()
	sz := fstat.Size()
	if src.Len != 0 {
		sz = src.Len
		dst += fmt.Sprintf(".%d-%d", src.Off, src.Len)
	}
	l.Debug("uploading: %s %s", src, fmtSize(sz))

	_, err = Upload(ctx, &src, stg, compression, compressLevel, dst, fstat.Size())
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
