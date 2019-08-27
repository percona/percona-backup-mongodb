package backup

import (
	"context"
	"io"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona/percona-backup-mongodb/pbm"
)

// OplogTailer is used for reading the Mongodb oplog
type OplogTailer struct {
	pbm     *pbm.PBM
	node    *pbm.Node
	startTS primitive.Timestamp
	lastTS  primitive.Timestamp
	stopAt  primitive.Timestamp

	data       chan []byte // oplog data
	unreadData []byte      // reminded data that didn't fit into the buffer of previous read
}

// OplogTail creates a new OplogTailer instance
func OplogTail(pbmO *pbm.PBM, node *pbm.Node) *OplogTailer {
	return &OplogTailer{
		pbm:  pbmO,
		node: node,
		data: make(chan []byte),
	}
}

// Read impplements Reader interface.
// It reads data into from data channel. And returns io.EOF
// when the channel is closed. But before
// io.EOF it has to read all reminded data from previous reads.
func (ot *OplogTailer) Read(p []byte) (int, error) {
	var size int

	if len(ot.unreadData) > 0 {
		ot.unreadData, size = readInto(p, ot.unreadData)
		return size, nil
	}

	data := <-ot.data
	if data == nil {
		return 0, io.EOF
	}

	ot.unreadData, size = readInto(p, data)
	return size, nil
}

// readInto reads bytes from src into dst.
// It returns unread bytes from src (if there are) and the number of bytes read.
func readInto(dst, src []byte) (rmd []byte, n int) {
	n = len(src)
	if len(dst) < n {
		n = len(dst)
		rmd = make([]byte, len(src)-n)
		copy(rmd, src[n:])
	}

	copy(dst, src[:n])

	return rmd, n
}

// Run starts tailing the oplog data until the context beign canceled.
// Returned chan is need to indicate
// To read data use Read method.
func (ot *OplogTailer) Run(ctx context.Context) error {
	clName, err := ot.collectionName()
	if err != nil {
		return errors.Wrap(err, "determine oplog collection name")
	}
	cl := ot.pbm.Conn.Database("local").Collection(clName)

	cur, err := cl.Find(ctx, ot.mongoTailFilter(), options.Find().SetCursorType(options.Tailable))
	if err != nil {
		return errors.Wrap(err, "get the oplog cursor")
	}
	go func() {
		defer close(ot.data)
		defer cur.Close(ctx)

		for cur.Next(ctx) {
			ot.lastTS.T, ot.lastTS.I = cur.Current.Lookup("ts").Timestamp()
			if ot.startTS.T == 0 {
				ot.startTS = ot.lastTS
			}
			if ot.stopAt.T > 0 && primitive.CompareTimestamp(ot.stopAt, ot.lastTS) > 0 {
				return
			}

			select {
			case ot.data <- []byte(cur.Current.String()):
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

// mongoTailFilter returns a bson.M query filter for the oplog tail
// Criteria:
//   1. If 'lastTS' is defined, tail all non-noop oplogs with 'ts' $gt that ts
//   2. Or, if 'startTS' is defined, tail all non-noop oplogs with 'ts' $gte that ts
//   3. Or, tail all non-noop oplogs with 'ts' $gt 'lastWrite.OpTime.Ts' from the result of the "isMaster" mongodb server command
//   4. Or, tail all non-noop oplogs with 'ts' $gte now.
func (ot *OplogTailer) mongoTailFilter() bson.M {
	query := bson.M{"op": bson.M{"$ne": pbm.OperationNoop}}

	if ot.lastTS.T > 0 {
		query["ts"] = bson.M{"$gt": ot.lastTS}
		return query
	} else if ot.startTS.T > 0 {
		query["ts"] = bson.M{"$gte": ot.startTS}
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
