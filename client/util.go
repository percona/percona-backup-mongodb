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

func connect(ctx context.Context, uri, appName string) (*mongo.Client, error) {
	opts := options.Client().ApplyURI(uri).
		SetAppName(appName).
		SetReadPreference(readpref.Primary()).
		SetReadConcern(readconcern.Majority()).
		SetWriteConcern(writeconcern.New(writeconcern.WMajority()))

	return mongo.Connect(ctx, opts)
}

func lookupLeaderURI(ctx context.Context, uri, appName string) (string, error) {
	parsedURI, err := url.Parse(uri)
	if err != nil {
		return "", errors.WithMessage(err, "parse uri")
	}

	client, err := connect(ctx, uri, appName)
	if err != nil {
		return "", errors.WithMessage(err, "connect")
	}
	defer client.Disconnect(context.Background())

	node, err := nodeInfo(ctx, client)
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

func nodeInfo(ctx context.Context, m *mongo.Client) (*pbm.NodeInfo, error) {
	// TODO: "hello" since 5.0 (and 4.4.2, 4.2.10, 4.0.21, and 3.6.21)
	res := m.Database(pbm.DB).RunCommand(ctx, bson.D{{"isMaster", 1}})
	if err := res.Err(); err != nil {
		return nil, errors.WithMessage(err, "query")
	}

	info := new(pbm.NodeInfo)
	return info, errors.WithMessage(res.Decode(info), "decode")
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
