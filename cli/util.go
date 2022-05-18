package cli

import (
	"context"
	"strings"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
)

func parseRSNamesMapping(s string) (map[string]string, error) {
	rv := make(map[string]string)

	if s == "" {
		return rv, nil
	}

	checkSrc, checkDst := makeStrSet(), makeStrSet()

	for _, a := range strings.Split(s, ",") {
		m := strings.Split(a, "=")
		if len(m) != 2 {
			return nil, errors.Errorf("malformatted: %q", a)
		}

		src, dst := m[0], m[1]

		if checkSrc(src) {
			return nil, errors.Errorf("source %v is duplicated", src)
		}
		if checkDst(dst) {
			return nil, errors.Errorf("target %v is duplicated", dst)
		}

		rv[dst] = src
	}

	return rv, nil
}

func makeStrSet() func(string) bool {
	m := make(map[string]struct{})

	return func(s string) bool {
		if _, ok := m[s]; ok {
			return true
		}

		m[s] = struct{}{}
		return false
	}
}

func runningOp(ctx context.Context, m *mongo.Client) (*pbm.LockHeader, error) {
	locks, err := getLocks(ctx, m, &pbm.LockHeader{})
	if err != nil {
		return nil, errors.WithMessage(err, "get locks")
	}

	ts, err := clusterTime(ctx, m)
	if err != nil {
		return nil, errors.WithMessage(err, "get cluster time")
	}

	// Stop if there is some live operation.
	// But in case of stale lock just move on
	// and leave it for agents to deal with.
	for _, l := range locks {
		if l.Heartbeat.T+pbm.StaleFrameSec >= ts.T {
			return &l.LockHeader, nil
		}
	}

	return nil, nil
}

func getLocks(ctx context.Context, m *mongo.Client, header *pbm.LockHeader) ([]pbm.LockData, error) {
	cur, err := coll(m, pbm.LockCollection).Find(ctx, header)
	if err != nil {
		return nil, errors.WithMessage(err, "query")
	}

	locks := make([]pbm.LockData, 0)
	for cur.Next(ctx) {
		var l pbm.LockData

		if err := cur.Decode(&l); err != nil {
			return nil, errors.WithMessage(err, "decode")
		}

		locks = append(locks, l)
	}

	if err := cur.Err(); err != nil {
		return nil, errors.WithMessage(err, "cursor")
	}

	return locks, nil
}

func clusterTime(ctx context.Context, m *mongo.Client) (primitive.Timestamp, error) {
	var rv primitive.Timestamp

	// Make a read to force the cluster timestamp update.
	// Otherwise, cluster timestamp could remain the same between node info reads,
	// while in fact time has been moved forward.
	err := coll(m, pbm.LockCollection).FindOne(ctx, bson.M{}).Err()
	if err != nil && err != mongo.ErrNoDocuments {
		return rv, errors.WithMessage(err, "void read")
	}

	info, err := nodeInfo(ctx, m)
	if err != nil {
		return rv, errors.WithMessage(err, "get node info")
	}

	if info.ClusterTime == nil {
		return rv, errors.WithMessage(err, "no clusterTime in response")
	}

	rv = info.ClusterTime.ClusterTime
	return rv, nil
}

func coll(m *mongo.Client, name string) *mongo.Collection {
	return m.Database(pbm.DB).Collection(name)
}

func nodeInfo(ctx context.Context, m *mongo.Client) (*pbm.NodeInfo, error) {
	// TODO: "hello" since 5.0 (and 4.4.2, 4.2.10, 4.0.21, and 3.6.21)
	res := m.Database(pbm.DB).RunCommand(ctx, bson.D{{"isMaster", 1}})
	if err := res.Err(); err != nil {
		return nil, errors.WithMessage(err, "query")
	}

	info := new(pbm.NodeInfo)
	return info, errors.WithMessage(res.Decode(info), "decode")
}
