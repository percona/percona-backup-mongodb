package connect

import (
	"net/url"
	"strings"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/percona/percona-backup-mongodb/internal/context"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
)

type ConnectOptions struct {
	AppName string
}

func connect(ctx context.Context, uri, appName string) (*mongo.Client, error) {
	client, err := mongo.Connect(ctx,
		options.Client().ApplyURI(uri).
			SetAppName(appName).
			SetReadPreference(readpref.Primary()).
			SetReadConcern(readconcern.Majority()).
			SetWriteConcern(writeconcern.Majority()),
	)
	if err != nil {
		return nil, errors.Wrap(err, "mongo connect")
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "mongo ping")
	}

	return client, nil
}

type leadClient struct {
	client *mongo.Client
}

func UnsafeLeadClient(m *mongo.Client) MetaClient {
	return &leadClient{m}
}

func Connect(ctx context.Context, uri string, opts *ConnectOptions) (MetaClient, error) {
	if opts == nil {
		opts = &ConnectOptions{}
	}

	uri = "mongodb://" + strings.Replace(uri, "mongodb://", "", 1)

	client, err := connect(ctx, uri, opts.AppName)
	if err != nil {
		return nil, errors.Wrap(err, "create mongo connection")
	}

	inf, err := getNodeInfoExt(ctx, client)
	if err != nil {
		return nil, errors.Wrap(err, "get topology")
	}

	if !inf.isSharded() || inf.isConfigsvr() {
		return &leadClient{client: client}, nil
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
	client, err = connect(ctx, curi.String(), opts.AppName)
	if err != nil {
		return nil, errors.Wrapf(err, "create mongo connection to configsvr with connection string '%s'", curi)
	}

	return &leadClient{client: client}, nil
}

func (l *leadClient) Disconnect(ctx context.Context) error {
	return l.client.Disconnect(ctx)
}

func (l *leadClient) UnsafeClient() *mongo.Client {
	return l.client
}

func (l *leadClient) ConfigDatabase() *mongo.Database {
	return l.client.Database("config")
}

func (l *leadClient) AdminCommand(ctx context.Context, cmd any, opts ...*options.RunCmdOptions) *mongo.SingleResult {
	return l.client.Database(defs.DB).RunCommand(ctx, cmd, opts...)
}

func (l *leadClient) LogCollection() *mongo.Collection {
	return l.client.Database(defs.DB).Collection(defs.LogCollection)
}

func (l *leadClient) ConfigCollection() *mongo.Collection {
	return l.client.Database(defs.DB).Collection(defs.ConfigCollection)
}

func (l *leadClient) LockCollection() *mongo.Collection {
	return l.client.Database(defs.DB).Collection(defs.LockCollection)
}

func (l *leadClient) LockOpCollection() *mongo.Collection {
	return l.client.Database(defs.DB).Collection(defs.LockOpCollection)
}

func (l *leadClient) BcpCollection() *mongo.Collection {
	return l.client.Database(defs.DB).Collection(defs.BcpCollection)
}

func (l *leadClient) RestoresCollection() *mongo.Collection {
	return l.client.Database(defs.DB).Collection(defs.RestoresCollection)
}

func (l *leadClient) CmdStreamCollection() *mongo.Collection {
	return l.client.Database(defs.DB).Collection(defs.CmdStreamCollection)
}

func (l *leadClient) PITRChunksCollection() *mongo.Collection {
	return l.client.Database(defs.DB).Collection(defs.PITRChunksCollection)
}

func (l *leadClient) PBMOpLogCollection() *mongo.Collection {
	return l.client.Database(defs.DB).Collection(defs.PBMOpLogCollection)
}

func (l *leadClient) AgentsStatusCollection() *mongo.Collection {
	return l.client.Database(defs.DB).Collection(defs.AgentsStatusCollection)
}

type MetaClient interface {
	Disconnect(ctx context.Context) error

	UnsafeClient() *mongo.Client

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
