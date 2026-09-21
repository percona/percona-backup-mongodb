// PBM 2.x package
package backup

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
	"github.com/percona/percona-backup-mongodb/x/pbm/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

const (
	cursorCreateRetries               = 10
	openConflictWithCheckpointErrCode = 50915
	oplogRolledOverErrCode            = 50917
)

type Meta struct {
	ID           UUID           `bson:"backupId"`
	DBpath       string         `bson:"dbpath"`
	OplogStart   BCoplogTS      `bson:"oplogStart"`
	OplogEnd     BCoplogTS      `bson:"oplogEnd"`
	CheckpointTS bson.Timestamp `bson:"checkpointTimestamp"`
}

type BCoplogTS struct {
	TS bson.Timestamp `bson:"ts"`
	T  int64          `bson:"t"`
}

// see https://www.percona.com/blog/2021/06/07/experimental-feature-backupcursorextend-in-percona-server-for-mongodb/
type BackupCursorData struct {
	Meta *Meta
	Data []File
}

type BackupCursor struct {
	id    UUID
	conn  *mongo.Client
	opts  bson.D
	close chan struct{}

	CustomThisID string
}

func NewBackupCursor(conn *mongo.Client, opts bson.D) *BackupCursor {
	return &BackupCursor{
		conn: conn,
		opts: opts,
	}
}

var errTriesLimitExceeded = errors.New("tries limit exceeded")

func (bc *BackupCursor) create(ctx context.Context, retry int) (*mongo.Cursor, error) {
	opts := bc.opts
	for i := range retry {
		if i != 0 {
			// on retry, make new thisBackupName
			// otherwise, WT error: "Incremental identifier already exists"
			opts = make(bson.D, len(bc.opts))
			for j, a := range bc.opts {
				val := a.Value
				if a.Key == "thisBackupName" {
					bc.CustomThisID = fmt.Sprintf("%s.%d", a.Value, i)
					val = bc.CustomThisID
				}

				opts[j] = bson.E{a.Key, val}
			}
		}

		cur, err := bc.conn.Database("admin").Aggregate(ctx, mongo.Pipeline{
			{{"$backupCursor", opts}},
		})
		if err != nil {
			se, ok := err.(mongo.ServerError) //nolint:errorlint
			if !ok {
				return nil, err
			}

			retryableErr := false
			if se.HasErrorCode(openConflictWithCheckpointErrCode) {
				// {code: 50915,name: BackupCursorOpenConflictWithCheckpoint, categories: [RetriableError]}
				// https://github.com/percona/percona-server-mongodb/blob/psmdb-6.0.6-5/src/mongo/base/error_codes.yml#L526
				log.Printf("a checkpoint took place, retrying: %d/%d", i+1, retry)
				retryableErr = true
			} else if se.HasErrorCode(oplogRolledOverErrCode) {
				log.Printf("oplog rolled over while establishing the backup cursor, retrying: %d/%d", i+1, retry)
				retryableErr = true
			}

			if retryableErr {
				// don't sleep on the last retry attempt
				if i < retry-1 {
					time.Sleep(time.Second * time.Duration(i+1))
				}
				continue
			}

			return nil, err
		}

		return cur, nil
	}

	return nil, errTriesLimitExceeded
}

//nolint:nonamedreturns
func (bc *BackupCursor) Data(ctx context.Context) (_ *BackupCursorData, err error) {
	cur, err := bc.create(ctx, cursorCreateRetries)
	if err != nil {
		return nil, errors.Wrap(err, "create backupCursor")
	}
	defer func() {
		if err != nil {
			cur.Close(context.Background())
		}
	}()

	var m *Meta
	var files []File
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

		var d File
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
				log.Printf("stop cursor polling: %v, cursor err: %v",
					cur.Close(context.Background()), cur.Err()) // `ctx` is already canceled, so use a background context
				return
			case <-tk.C:
				cur.TryNext(ctx)
			}
		}
	}()

	return &BackupCursorData{m, files}, nil
}

func (bc *BackupCursor) Journals(upto bson.Timestamp) ([]File, error) {
	ctx := context.Background()
	cur, err := bc.conn.Database("admin").Aggregate(ctx,
		mongo.Pipeline{
			{{"$backupCursorExtend", bson.D{{"backupId", bc.id}, {"timestamp", upto}}}},
		})
	if err != nil {
		return nil, errors.Wrap(err, "create backupCursorExtend")
	}
	defer cur.Close(ctx)

	var j []File

	err = cur.All(ctx, &j)
	return j, err
}

func (bc *BackupCursor) Close() {
	if bc.close != nil {
		close(bc.close)
	}
}

func backupCursorName(s string) string {
	return strings.NewReplacer("-", "", ":", "").Replace(s)
}

const storagebson = "storage.bson"

func getStorageBSON(dbpath string) (*File, error) {
	f, err := os.Stat(path.Join(dbpath, storagebson))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = storage.ErrNotExist
		}
		return nil, err
	}

	return &File{
		Name:  path.Join(dbpath, storagebson),
		Len:   f.Size(),
		Size:  f.Size(),
		Fmode: f.Mode(),
	}, nil
}

// UUID represents a UUID as saved in MongoDB
type UUID struct{ uuid.UUID }

// MarshalBSONValue implements the bson.ValueMarshaler interface.
func (id UUID) MarshalBSONValue() (byte, []byte, error) {
	return byte(bson.TypeBinary), bsoncore.AppendBinary(nil, 4, id.UUID[:]), nil
}

// UnmarshalBSONValue implements the bson.ValueUnmarshaler interface.
func (id *UUID) UnmarshalBSONValue(t byte, raw []byte) error {
	if t != byte(bson.TypeBinary) {
		return errors.New("invalid format on unmarshal bson value")
	}

	_, data, _, ok := bsoncore.ReadBinary(raw)
	if !ok {
		return errors.New("not enough bytes to unmarshal bson value")
	}

	copy(id.UUID[:], data)

	return nil
}

// IsZero implements the bson.Zeroer interface.
func (id *UUID) IsZero() bool {
	return bytes.Equal(id.UUID[:], uuid.Nil[:])
}

// trimFilePrefix strips trimPrefix from fname and returns
// a cleaned relative path (e.g. `foo` rather than `/foo`).
func trimFilePrefix(fname, trimPrefix string) string {
	// path.Clean to get rid of `/` at the beginning in case it's
	// left after TrimPrefix. Just for consistent file names in metadata
	return path.Clean("./" + strings.TrimPrefix(fname, trimPrefix))
}
