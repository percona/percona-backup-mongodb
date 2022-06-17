package client

import (
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/percona/percona-backup-mongodb/pbm"
)

var ErrNotFound = errors.New("not found")

func connect(ctx context.Context, uri string) (*mongo.Client, error) {
	opts := options.Client().ApplyURI(uri).
		SetReadPreference(readpref.Primary()).
		SetReadConcern(readconcern.Majority()).
		SetWriteConcern(writeconcern.New(writeconcern.WMajority()))

	if v := ctx.Value(AppNameCtxKey); v != nil {
		opts.SetAppName(v.(string))
	}

	return mongo.Connect(ctx, opts)
}

func lookupLeaderURI(ctx context.Context, uri string) (string, error) {
	parsedURI, err := url.Parse(uri)
	if err != nil {
		return "", errors.WithMessage(err, "parse uri")
	}

	client, err := connect(ctx, uri)
	if err != nil {
		return "", errors.WithMessage(err, "connect")
	}
	defer client.Disconnect(context.Background())

	node, err := hello(ctx, client)
	if err != nil {
		return "", errors.WithMessage(err, "get node info")
	}

	if !node.IsLeader() {
		cfgSrvHost, err := configSrvHost(ctx, client)
		if err != nil {
			return "", errors.WithMessage(err, "get configsrv host")
		}

		parsedURI.Host = cfgSrvHost
	}

	q := parsedURI.Query()
	q.Del("replicaSet")
	parsedURI.RawQuery = q.Encode()
	return parsedURI.String(), nil
}

func configSrvHost(ctx context.Context, m *mongo.Client) (string, error) {
	opts := options.FindOne().SetProjection(bson.M{"_id": 0, "configsvrConnectionString": 1})
	res := coll(m, "system.version").FindOne(ctx, bson.M{"_id": "shardIdentity"}, opts)
	if err := res.Err(); err != nil {
		return "", errors.WithMessage(err, "query")
	}

	type shardIdentity struct {
		URI string `bson:"configsvrConnectionString"`
	}

	shId := new(shardIdentity)
	if err := res.Decode(shId); err != nil {
		return "", errors.WithMessage(err, "decode")
	}

	_, host, ok := strings.Cut(shId.URI, "/")
	if !ok {
		return "", errors.Errorf("unrecognised uri %q", shId.URI)
	}

	return host, nil
}

func coll(m *mongo.Client, name string) *mongo.Collection {
	return m.Database(pbm.DB).Collection(name)
}

func newCmd(cmd pbm.Command) *pbm.Cmd {
	return &pbm.Cmd{
		Cmd: cmd,
		TS:  time.Now().UTC().Unix(),
	}
}

func sendCommand(ctx context.Context, m *mongo.Client, cmd *pbm.Cmd) (primitive.ObjectID, error) {
	res, err := coll(m, pbm.CmdStreamCollection).InsertOne(ctx, cmd)
	if err != nil {
		return primitive.NilObjectID, err
	}

	return res.InsertedID.(primitive.ObjectID), nil
}

func getCommand(ctx context.Context, m *mongo.Client, id primitive.ObjectID) (*pbm.Cmd, error) {
	res := coll(m, pbm.CmdStreamCollection).FindOne(ctx, bson.D{{"_id", id}})
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrNotFound
		}

		return nil, errors.WithMessage(err, "query")
	}

	cmd := new(pbm.Cmd)
	err := res.Decode(cmd)
	return cmd, errors.WithMessage(err, "decode")
}

func waitForCmdStatus(ctx context.Context, m *mongo.Client, id primitive.ObjectID, statuses ...pbm.Status) error {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			cmd, err := getCommand(ctx, m, id)
			if err != nil {
				return err
			}

			for _, cond := range cmd.Status.Conditions {
				for _, s := range statuses {
					if cond.Status == s {
						return nil
					}
				}
			}
		}
	}
}

func waitForOpLockAcquired(ctx context.Context, m *mongo.Client, h *pbm.LockHeader) error {
	t := time.NewTimer(time.Second)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			_, err := getLock(ctx, m, h)
			if err == nil {
				continue
			}

			if !errors.Is(err, mongo.ErrNoDocuments) {
				return err
			}

			return nil
		}
	}
}

func waitForOpLockReleased(ctx context.Context, m *mongo.Client, h *pbm.LockHeader) error {
	t := time.NewTimer(time.Second)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			lock, err := getLock(ctx, m, h)
			if err != nil {
				if errors.Is(err, mongo.ErrNoDocuments) {
					return nil
				}
				return err
			}
			ct, err := clusterTime(ctx, m)
			if err != nil {
				return errors.WithMessage(err, "get cluster time")
			}

			if lock.Heartbeat.T+pbm.StaleFrameSec < ct.T {
				return errors.WithMessagef(err,
					"operation %q stale, last beat ts: %d",
					h.Type, lock.Heartbeat.T)
			}
		}
	}
}

func getLock(ctx context.Context, m *mongo.Client, h *pbm.LockHeader) (*pbm.LockData, error) {
	res := coll(m, pbm.LockCollection).FindOne(ctx, h)
	if err := res.Err(); err != nil {
		return nil, errors.WithMessage(err, "query")
	}

	rv := new(pbm.LockData)
	err := res.Decode(rv)
	return rv, errors.WithMessage(err, "decode")
}

func waitForOp(ctx context.Context, m *mongo.Client, cmd pbm.Command) error {
	limit, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	h := &pbm.LockHeader{Type: cmd}
	if err := waitForOpLockAcquired(limit, m, h); err != nil &&
		!errors.Is(err, context.DeadlineExceeded) {
		return errors.WithMessage(err, "wait for oplock acquired")
	}
	if err := waitForOpLockReleased(ctx, m, h); err != nil {
		return errors.WithMessage(err, "wait for oplock released")
	}

	return nil
}

func runMCmd(ctx context.Context, m *mongo.Client, cmd string) *mongo.SingleResult {
	return m.Database(pbm.DB).RunCommand(ctx, bson.D{{cmd, 1}})
}

func groupBy[K comparable, V any](xs []V, key func(*V) K) map[K][]V {
	rv := make(map[K][]V)

	for i := range xs {
		k := key(&xs[i])
		rv[k] = append(rv[k], xs[i])
	}

	return rv
}

func keyBy[K comparable, V any](xs []V, key func(*V) K) map[K]V {
	rv := make(map[K]V)

	for i := range xs {
		k := key(&xs[i])
		rv[k] = xs[i]
	}

	return rv
}
