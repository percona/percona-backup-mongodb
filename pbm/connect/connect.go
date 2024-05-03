package connect

import (
	"context"
	"net/url"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

type MongoOption func(*options.ClientOptions) error

func AppName(name string) MongoOption {
	return func(opts *options.ClientOptions) error {
		if len(name) == 0 {
			return errors.New("AppName is not specified")
		}
		opts.SetAppName(name)
		return nil
	}
}

func Direct(direct bool) MongoOption {
	return func(opts *options.ClientOptions) error {
		opts.SetDirect(direct)
		return nil
	}
}

// ReadConcern option sets availability guarantees for read operation.
// For PBM typically use: [readconcern.Local] or [readconcern.Majority].
// If the option is not specified the default is: [readconcern.Majority].
func ReadConcern(readConcern *readconcern.ReadConcern) MongoOption {
	return func(opts *options.ClientOptions) error {
		if readConcern == nil {
			return errors.New("ReadConcern not specified")
		}
		opts.SetReadConcern(readConcern)
		return nil
	}
}

// WriteConcern option sets level of acknowledgment for write operation.
// For PBM typically use: [writeconcern.W1] or [writeconcern.Majority].
// If the option is not specified the default is: [writeconcern.Majority].
func WriteConcern(writeConcern *writeconcern.WriteConcern) MongoOption {
	return func(opts *options.ClientOptions) error {
		if writeConcern == nil {
			return errors.New("WriteConcern not specified")
		}
		opts.SetWriteConcern(writeConcern)
		return nil
	}
}

// NoRS option removes replica set name setting
func NoRS() MongoOption {
	return func(opts *options.ClientOptions) error {
		opts.SetReplicaSet("")
		return nil
	}
}

func ConnectTimeout(d time.Duration) MongoOption {
	return func(opts *options.ClientOptions) error {
		opts.SetConnectTimeout(d)
		return nil
	}
}

func ServerSelectionTimeout(d time.Duration) MongoOption {
	return func(opts *options.ClientOptions) error {
		opts.SetServerSelectionTimeout(d)
		return nil
	}
}

func MongoConnectWithOpts(ctx context.Context,
	uri string,
	mongoOptions ...MongoOption,
) (*mongo.Client, *options.ClientOptions, error) {
	if !strings.HasPrefix(uri, "mongodb://") {
		uri = "mongodb://" + uri
	}

	// default options
	mopts := options.Client().
		SetAppName("pbm").
		SetReadPreference(readpref.Primary()).
		SetReadConcern(readconcern.Majority()).
		SetWriteConcern(writeconcern.Majority()).
		SetDirect(false)

	// apply and override using end-user options from conn string
	mopts.ApplyURI(uri)

	// override with explicit options from the code
	for _, opt := range mongoOptions {
		if opt != nil {
			if err := opt(mopts); err != nil {
				return nil, nil, errors.Wrap(err, "invalid mongo option")
			}
		}
	}

	conn, err := mongo.Connect(ctx, mopts)
	if err != nil {
		return nil, nil, errors.Wrap(err, "connect")
	}

	err = conn.Ping(ctx, nil)
	if err != nil {
		return nil, nil, errors.Wrap(err, "ping")
	}

	return conn, mopts, nil
}

func MongoConnect(
	ctx context.Context,
	uri string,
	mongoOptions ...MongoOption,
) (*mongo.Client, error) {
	client, _, err := MongoConnectWithOpts(ctx, uri, mongoOptions...)
	return client, err
}

type clientImpl struct {
	client  *mongo.Client
	options *options.ClientOptions
}

func UnsafeClient(m *mongo.Client) Client {
	return &clientImpl{
		client:  m,
		options: options.Client(),
	}
}

// Connect resolves MongoDB connection to Primary member and wraps it within Client object.
// In case of replica set it returns connection to Primary member,
// while in case of sharded cluster it returns connection to Config RS Primary member.
func Connect(ctx context.Context, uri, appName string) (Client, error) {
	client, opts, err := MongoConnectWithOpts(ctx, uri, AppName(appName))
	if err != nil {
		return nil, errors.Wrap(err, "create mongo connection")
	}

	inf, err := getNodeInfo(ctx, client)
	if err != nil {
		return nil, errors.Wrap(err, "get NodeInfo")
	}
	if inf.isMongos() {
		return &clientImpl{
			client:  client,
			options: opts,
		}, nil
	}

	inf.Opts, err = getMongodOpts(ctx, client, nil)
	if err != nil {
		return nil, errors.Wrap(err, "get mongod options")
	}

	if inf.isClusterLeader() {
		return &clientImpl{
			client:  client,
			options: opts,
		}, nil
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
	curi.Host = chost[1]
	client, err = MongoConnect(ctx, curi.String(), AppName(appName), NoRS())
	if err != nil {
		return nil, errors.Wrapf(err, "create mongo connection to configsvr with connection string '%s'", curi)
	}

	return &clientImpl{
		client:  client,
		options: opts,
	}, nil
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

func (l *clientImpl) MongoOptions() *options.ClientOptions {
	return l.options
}

func (l *clientImpl) ConfigDatabase() *mongo.Database {
	return l.client.Database("config")
}

func (l *clientImpl) AdminCommand(ctx context.Context, cmd bson.D, opts ...*options.RunCmdOptions) *mongo.SingleResult {
	cmd = append(cmd,
		bson.E{"readConcern", l.options.ReadConcern},
		bson.E{"writeConcern", l.options.WriteConcern},
	)
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
	MongoOptions() *options.ClientOptions

	ConfigDatabase() *mongo.Database
	AdminCommand(ctx context.Context, cmd bson.D, opts ...*options.RunCmdOptions) *mongo.SingleResult

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
