package backup

import (
	"context"
	"io"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/pbm"
)

// Oplog is used for reading the Mongodb oplog
type Oplog struct {
	node *pbm.Node
}

// NewOplog creates a new Oplog instance
func NewOplog(node *pbm.Node) *Oplog {
	return &Oplog{
		node: node,
	}
}

// SliceTo slice oplog starting from given timestamp
func (ot *Oplog) SliceTo(ctx context.Context, w io.Writer, fromTs primitive.Timestamp) error {
	clName, err := ot.collectionName()
	if err != nil {
		return errors.Wrap(err, "determine oplog collection name")
	}
	cl := ot.node.Session().Database("local").Collection(clName)

	cur, err := cl.Find(ctx, bson.M{
		"op": bson.M{"$ne": pbm.OperationNoop},
		"ts": bson.M{"$gte": fromTs},
	})
	if err != nil {
		return errors.Wrap(err, "get the oplog cursor")
	}
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		_, err = w.Write([]byte(cur.Current))
		if err != nil {
			return errors.Wrap(err, "write to pipe")
		}
	}

	return cur.Err()
}

// StartTS returns timestamp the oplog should be read from later on
func (ot *Oplog) StartTS() (primitive.Timestamp, error) {
	isMaster, err := ot.node.GetIsMaster()
	if err != nil {
		return primitive.Timestamp{}, errors.Wrap(err, "get isMaster data")
	}

	return isMaster.LastWrite.OpTime.TS, nil
}

func (ot *Oplog) collectionName() (string, error) {
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
