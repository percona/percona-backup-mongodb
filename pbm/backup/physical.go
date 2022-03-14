package backup

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	plog "github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

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

type BackupCursorData struct {
	Meta *Meta
	Data []pbm.File
}
type BackupCursor struct {
	id    UUID
	n     *pbm.Node
	l     *plog.Event
	close chan struct{}
}

func NewBackupCursor(n *pbm.Node, l *plog.Event) *BackupCursor {
	return &BackupCursor{
		n: n,
		l: l,
	}
}

func (bc *BackupCursor) Data(ctx context.Context) (bcp *BackupCursorData, err error) {
	cur, err := bc.n.Session().Database("admin").Aggregate(ctx, mongo.Pipeline{
		{{"$backupCursor", bson.D{}}},
	})
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
					cur.Close(ctx), cur.Err())
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

func (b *Backup) doPhysical(ctx context.Context, bcp pbm.BackupCmd, opid pbm.OPID, rsMeta *pbm.BackupReplset, inf *pbm.NodeInfo, stg storage.Storage, l *plog.Event) error {
	cursor := NewBackupCursor(b.node, l)
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

	l.Info("uploading files")
	subdir := bcp.Name + "/" + rsMeta.Name
	for _, bd := range append(bcur.Data, jrnls...) {
		select {
		case <-ctx.Done():
			return ErrCancelled
		default:
		}

		f, err := writeFile(ctx, bd.Name, subdir+"/"+strings.TrimPrefix(bd.Name, bcur.Meta.DBpath+"/"), stg, bcp.Compression, bcp.CompressionLevel, l)
		if err != nil {
			return errors.Wrapf(err, "upload file `%s`", bd.Name)
		}
		f.Name = strings.TrimPrefix(bd.Name, bcur.Meta.DBpath+"/")
		rsMeta.Files = append(rsMeta.Files, *f)
	}
	l.Info("uploading done")

	err = b.cn.RSSetPhyFiles(bcp.Name, rsMeta.Name, rsMeta.Files)
	if err != nil {
		return errors.Wrap(err, "set shard's files list")
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

func writeFile(ctx context.Context, src, dst string, stg storage.Storage, compression pbm.CompressionType, compressLevel *int, l *plog.Event) (*pbm.File, error) {
	fstat, err := os.Stat(src)
	if err != nil {
		return nil, errors.Wrap(err, "get file stat")
	}

	l.Debug("uploading: %s %s", src, fmtSize(fstat.Size()))

	dst += compression.Suffix()

	_, err = Upload(ctx, &file{src}, stg, compression, compressLevel, dst, int(fstat.Size()))
	if err != nil {
		return nil, errors.Wrap(err, "upload file")
	}

	finf, err := stg.FileStat(dst)
	if err != nil {
		return nil, errors.Wrapf(err, "get storage file stat %s", dst)
	}

	return &pbm.File{
		Name:    src,
		Size:    fstat.Size(),
		Fmode:   fstat.Mode(),
		StgSize: finf.Size,
	}, nil
}

type file struct {
	path string
}

func (f *file) WriteTo(w io.Writer) (int64, error) {
	fd, err := os.Open(f.path)
	if err != nil {
		return 0, errors.Wrap(err, "open file for reading")
	}
	defer fd.Close()

	return io.Copy(w, fd)
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
