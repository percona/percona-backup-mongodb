package connect

import (
	"context"
	"net/url"
	"strings"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
)

type ConnectOptions struct {
	AppName string
}

type MongoConnectOptions struct {
	AppName string
	Direct  bool
}

func MongoConnect(ctx context.Context, uri string, opts *MongoConnectOptions) (*mongo.Client, error) {
	if !strings.HasPrefix(uri, "mongodb://") {
		uri = "mongodb://" + uri
	}

	if opts == nil {
		opts = &MongoConnectOptions{}
	}

	mopts := options.Client().ApplyURI(uri).
		SetAppName(opts.AppName).
		SetReadPreference(readpref.Primary()).
		SetReadConcern(readconcern.Majority()).
		SetWriteConcern(writeconcern.Majority()).
		SetDirect(opts.Direct)
	conn, err := mongo.Connect(ctx, mopts)
	if err != nil {
		return nil, errors.Wrap(err, "connect")
	}

	err = conn.Ping(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "ping")
	}

	return conn, nil
}

type clientImpl struct {
	client *mongo.Client
}

func UnsafeClient(m *mongo.Client) Client {
	return &clientImpl{m}
}

func Connect(ctx context.Context, uri string, opts *ConnectOptions) (Client, error) {
	if opts == nil {
		opts = &ConnectOptions{}
	}

	mopts := &MongoConnectOptions{AppName: opts.AppName}
	client, err := MongoConnect(ctx, uri, mopts)
	if err != nil {
		return nil, errors.Wrap(err, "create mongo connection")
	}

	inf, err := getNodeInfo(ctx, client)
	if err != nil {
		return nil, errors.Wrap(err, "get NodeInfo")
	}
	if inf.isMongos() {
		return &clientImpl{client: client}, nil
	}

	inf.Opts, err = getMongodOpts(ctx, client, nil)
	if err != nil {
		return nil, errors.Wrap(err, "get mongod options")
	}

	if inf.isClusterLeader() {
		return &clientImpl{client: client}, nil
	}

	csvr, err := getConfigsvrURI(ctx, client)
	if err != nil {
		return nil, errors.Wrap(err, "get config server connection URI")
	}
	// no need in this connection anymore, we need a new one with the ConfigServer
	err = client.Disconnect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "disconnect old client")
	}

	chost := strings.Split(csvr, "/")
	if len(chost) < 2 {
		return nil, errors.Wrapf(err, "define config server connection URI from %s", csvr)
	}

	curi, err := url.Parse(uri)
	if err != nil {
		return nil, errors.Wrapf(err, "parse mongo-uri '%s'", uri)
	}

	// Preserving the `replicaSet` parameter will cause an error
	// while connecting to the ConfigServer (mismatched replicaset names)
	query := curi.Query()
	query.Del("replicaSet")
	curi.RawQuery = query.Encode()
	curi.Host = chost[1]
	client, err = MongoConnect(ctx, curi.String(), mopts)
	if err != nil {
		return nil, errors.Wrapf(err, "create mongo connection to configsvr with connection string '%s'", curi)
	}

	return &clientImpl{client: client}, nil
}

func (l *clientImpl) HasValidConnection(ctx context.Context) error {
	err := l.client.Ping(ctx, readpref.Primary())
	if err != nil {
		return err
	}

	info, err := getNodeInfo(ctx, l.client)
	if err != nil {
		return errors.Wrap(err, "get node info ext")
	}

	if !info.isMongos() && !info.isClusterLeader() {
		return ErrInvalidConnection
	}

	return nil
}

func (l *clientImpl) Disconnect(ctx context.Context) error {
	return l.client.Disconnect(ctx)
}

func (l *clientImpl) MongoClient() *mongo.Client {
	return l.client
}

func (l *clientImpl) ConfigDatabase() *mongo.Database {
	return l.client.Database("config")
}

func (l *clientImpl) AdminCommand(ctx context.Context, cmd any, opts ...*options.RunCmdOptions) *mongo.SingleResult {
	return l.client.Database(defs.DB).RunCommand(ctx, cmd, opts...)
}

func (l *clientImpl) LogCollection() *mongo.Collection {
	return l.client.Database(defs.DB).Collection(defs.LogCollection)
}

func (l *clientImpl) ConfigCollection() *mongo.Collection {
	return l.client.Database(defs.DB).Collection(defs.ConfigCollection)
}

func (l *clientImpl) LockCollection() *mongo.Collection {
	return l.client.Database(defs.DB).Collection(defs.LockCollection)
}

func (l *clientImpl) LockOpCollection() *mongo.Collection {
	return l.client.Database(defs.DB).Collection(defs.LockOpCollection)
}

func (l *clientImpl) BcpCollection() *mongo.Collection {
	return l.client.Database(defs.DB).Collection(defs.BcpCollection)
}

func (l *clientImpl) RestoresCollection() *mongo.Collection {
	return l.client.Database(defs.DB).Collection(defs.RestoresCollection)
}

func (l *clientImpl) CmdStreamCollection() *mongo.Collection {
	return l.client.Database(defs.DB).Collection(defs.CmdStreamCollection)
}

func (l *clientImpl) PITRChunksCollection() *mongo.Collection {
	return l.client.Database(defs.DB).Collection(defs.PITRChunksCollection)
}

func (l *clientImpl) PBMOpLogCollection() *mongo.Collection {
	return l.client.Database(defs.DB).Collection(defs.PBMOpLogCollection)
}

func (l *clientImpl) AgentsStatusCollection() *mongo.Collection {
	return l.client.Database(defs.DB).Collection(defs.AgentsStatusCollection)
}

var ErrInvalidConnection = errors.New("invalid mongo connection")

type Client interface {
	Disconnect(ctx context.Context) error

	MongoClient() *mongo.Client

	ConfigDatabase() *mongo.Database
	AdminCommand(ctx context.Context, cmd any, opts ...*options.RunCmdOptions) *mongo.SingleResult

	LogCollection() *mongo.Collection
	ConfigCollection() *mongo.Collection
	LockCollection() *mongo.Collection
	LockOpCollection() *mongo.Collection
	BcpCollection() *mongo.Collection
	RestoresCollection() *mongo.Collection
	CmdStreamCollection() *mongo.Collection
	PITRChunksCollection() *mongo.Collection
	PBMOpLogCollection() *mongo.Collection
	AgentsStatusCollection() *mongo.Collection
}
