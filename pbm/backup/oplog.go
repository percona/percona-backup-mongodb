package backup

import (
	"context"
	"fmt"
	"io"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona/percona-backup-mongodb/pbm"
)

// Oplog is used for reading the Mongodb oplog
type Oplog struct {
	node  *pbm.Node
	start primitive.Timestamp
	end   primitive.Timestamp
}

// NewOplog creates a new Oplog instance
func NewOplog(node *pbm.Node) *Oplog {
	return &Oplog{
		node: node,
	}
}

// SetTailingSpan sets oplog tailing window
func (ot *Oplog) SetTailingSpan(start, end primitive.Timestamp) {
	ot.start = start
	ot.end = end
}

type ErrInsuffRange struct {
	t primitive.Timestamp
}

func (e ErrInsuffRange) Error() string {
	return fmt.Sprintf("oplog has insufficient range, some records since the last saved ts %v are missing. Run `pbm backup` to create a valid starting point for the PITR", e.t)
}

// WriteTo writes an oplog slice between start and end timestamps into the given io.Writer
//
// To be sure we have read ALL records up to the specified cluster time.
// Specifically, to be sure that no operations from the past gonna came after we finished the slicing,
// we have to tail until some record with ts > endTS. And it might be a noop.
func (ot *Oplog) WriteTo(w io.Writer) (int64, error) {
	if ot.start.T == 0 || ot.end.T == 0 {
		return 0, errors.Errorf("oplog TailingSpan should be set, have start: %v, end: %v", ot.start, ot.end)
	}

	ctx := context.Background()

	clName, err := ot.collectionName()
	if err != nil {
		return 0, errors.Wrap(err, "determine oplog collection name")
	}
	cl := ot.node.Session().Database("local").Collection(clName)

	cur, err := cl.Find(ctx,
		bson.M{
			"ts": bson.M{"$gte": ot.start},
		},
		options.Find().SetCursorType(options.Tailable),
	)
	if err != nil {
		return 0, errors.Wrap(err, "get the oplog cursor")
	}
	defer cur.Close(ctx)

	opts := primitive.Timestamp{}
	var ok, rcheck bool
	var written int64
	for cur.Next(ctx) {
		opts.T, opts.I, ok = cur.Current.Lookup("ts").TimestampOK()
		if !ok {
			return written, errors.Errorf("get the timestamp of record %v", cur.Current)
		}
		// Before processing the first oplog record we check if oplog has sufficient range,
		// i.e. if there are no gaps between the ts of the last backup or slice and
		// the first record of the current slice. Whereas the request is ">= last_saved_ts"
		// we don't know if the returned oldest record is the first since last_saved_ts or
		// there were other records that are now removed because of oplog collection capacity.
		// So after we retrieved the first record of the current slice we check if there is
		// at least one preceding record (basically if there is still record(s) from the previous set).
		// If so, we can be sure we have a contiguous history with respect to the last_saved_slice.
		//
		// We should do this check only after we retrieved the first record of the set. Otherwise,
		// there is a possibility some records would be erased in a time span between the check and
		// the first record retrieval due to ongoing write traffic (i.e. oplog append).
		// There's a chance of false-negative though.
		if !rcheck {
			c, err := cl.CountDocuments(ctx, bson.M{"ts": bson.M{"$lte": ot.start}}, options.Count().SetLimit(1))
			if err != nil {
				return 0, errors.Wrap(err, "check oplog range: count preceding documents")
			}
			if c == 0 {
				return 0, ErrInsuffRange{ot.start}
			}
			rcheck = true
		}

		if primitive.CompareTimestamp(ot.end, opts) == -1 {
			return written, nil
		}

		// skip noop operations
		if cur.Current.Lookup("op").String() == string(pbm.OperationNoop) {
			continue
		}

		n, err := w.Write([]byte(cur.Current))
		if err != nil {
			return written, errors.Wrap(err, "write to pipe")
		}
		written += int64(n)
	}

	return written, cur.Err()
}

// LastWrite returns a timestamp of the last write operation readable by majority reads
func (ot *Oplog) LastWrite() (primitive.Timestamp, error) {
	return pbm.LastWrite(ot.node.Session())
}

func (ot *Oplog) collectionName() (string, error) {
	inf, err := ot.node.GetInfo()
	if err != nil {
		return "", errors.Wrap(err, "get NodeInfo document")
	}

	if len(inf.Hosts) > 0 {
		return "oplog.rs", nil
	}
	if !inf.IsPrimary {
		return "", errors.New("not connected to primary")
	}
	return "oplog.$main", nil
}
