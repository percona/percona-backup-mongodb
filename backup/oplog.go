package backup

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona/percona-backup-mongodb/pbm"
)

type OplogTailer struct {
	pbm       *pbm.PBM
	node      *pbm.Node
	StartTS   primitive.Timestamp
	LastTS    primitive.Timestamp
	StopAt    primitive.Timestamp
	TotalSize uint64
	DocsCount uint64
}

func OplogTail(pbmO *pbm.PBM, node *pbm.Node) *OplogTailer {
	return &OplogTailer{
		pbm:  pbmO,
		node: node,
	}
}

func (ot *OplogTailer) Run() (<-chan []byte, error) {
	data := make(chan []byte)

	clName, err := ot.collectionName()
	if err != nil {
		return nil, errors.Wrap(err, "determine collection name")
	}
	ctx := context.Background()
	cl := ot.pbm.Conn.Database("local").Collection(clName)

	cur, err := cl.Find(ctx, ot.tailQuery(), options.Find().SetCursorType(options.Tailable))
	if err != nil {
		return nil, errors.Wrap(err, "get the oplog cursor")
	}
	go func() {
		defer close(data)
		defer cur.Close(ctx)

		for cur.Next(ctx) {
			ot.LastTS.T, ot.LastTS.I = cur.Current.Lookup("ts").Timestamp()
			if ot.StartTS.T == 0 {
				ot.StartTS = ot.LastTS
			}
			if ot.StopAt.T > 0 && primitive.CompareTimestamp(ot.StopAt, ot.LastTS) > 0 {
				return
			}

			data <- []byte(cur.Current.String())
		}

	}()

	return data, nil
}

// tailQuery returns a bson.M query filter for the oplog tail
// Criteria:
//   1. If 'StopTS' is defined, tail all non-noop oplogs with 'ts' $gt that ts
//   2. Or, if 'StartTS' is defined, tail all non-noop oplogs with 'ts' $gte that ts
//   3. Or, tail all non-noop oplogs with 'ts' $gt 'lastWrite.OpTime.Ts' from the result of the "isMaster" mongodb server command
//   4. Or, tail all non-noop oplogs with 'ts' $gte now.
func (ot *OplogTailer) tailQuery() bson.M {
	query := bson.M{"op": bson.M{"$ne": pbm.OperationNoop}}

	if ot.LastTS.T > 0 {
		query["ts"] = bson.M{"$gt": ot.LastTS}
		return query
	} else if ot.StartTS.T > 0 {
		query["ts"] = bson.M{"$gte": ot.StartTS}
		return query
	}

	isMaster, err := ot.node.GetIsMaster()
	if err != nil {
		query["ts"] = bson.M{"$gte": time.Now().Unix()}
		return query
	}

	query["ts"] = bson.M{"$gt": isMaster.LastWrite.OpTime.TS}
	return query
}

func (ot *OplogTailer) collectionName() (string, error) {
	isMaster, err := ot.node.GetIsMaster()
	if err != nil {
		return "", errors.Wrap(err, "get isMaster document")
	}

	if len(isMaster.Hosts) > 0 {
		return "oplog.rs", nil
	}
	if !isMaster.IsMaster {
		return "", errors.New("not connected to master")
	}
	return "oplog.$main", nil
}
