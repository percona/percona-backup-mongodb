package connect

import (
	"context"
	"log"
	"net/url"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/percona/percona-backup-mongodb/x/pbm/defs"
	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
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
		if err := validateReadConcern(readConcern); err != nil {
			return err
		}
		opts.SetReadConcern(readConcern)
		return nil
	}
}

func validateReadConcern(readConcern *readconcern.ReadConcern) error {
	if readConcern == nil {
		return errors.New("ReadConcern not specified")
	}
	if readConcern.Level != readconcern.Local().Level &&
		readConcern.Level != readconcern.Majority().Level {
		return errors.New("ReadConcern level is not allowed")
	}
	return nil
}

// WriteConcern option sets level of acknowledgment for write operation.
// For PBM typically use: [writeconcern.W1] or [writeconcern.Majority].
// If the option is not specified the default is: [writeconcern.Majority].
func WriteConcern(writeConcern *writeconcern.WriteConcern) MongoOption {
	return func(opts *options.ClientOptions) error {
		if err := validateWriteConcern(writeConcern); err != nil {
			return err
		}

		opts.SetWriteConcern(writeConcern)
		return nil
	}
}

func validateWriteConcern(writeConcern *writeconcern.WriteConcern) error {
	if writeConcern == nil {
		return errors.New("WriteConcern not specified")
	}
	if w, ok := writeConcern.W.(int); ok && w == 0 {
		return errors.New("WriteConcern without acknowledgment is not allowed (w: 0)")
	}
	return nil
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

// ensureMongoScheme ensures the URI has a mongodb:// scheme prefix.
// Returns an error if mongodb+srv:// scheme is used.
func ensureMongoScheme(uri string) (string, error) {
	if strings.HasPrefix(uri, "mongodb+srv://") {
		return "", errors.New("mongodb+srv:// URI scheme is not supported, use mongodb:// instead")
	}
	if !strings.HasPrefix(uri, "mongodb://") {
		uri = "mongodb://" + uri
	}
	return uri, nil
}

func MongoConnectWithOpts(ctx context.Context,
	uri string,
	mongoOptions ...MongoOption,
) (*mongo.Client, *options.ClientOptions, error) {
	uri, err := ensureMongoScheme(uri)
	if err != nil {
		return nil, nil, err
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
	if err := validateConnStringOpts(mopts); err != nil {
		return nil, nil, errors.Wrap(err, "invalid connection string option")
	}

	// override with explicit options from the code
	for _, opt := range mongoOptions {
		if opt != nil {
			if err := opt(mopts); err != nil {
				return nil, nil, errors.Wrap(err, "invalid mongo option")
			}
		}
	}

	conn, err := mongo.Connect(mopts)
	if err != nil {
		return nil, nil, errors.Wrap(err, "connect")
	}

	err = conn.Ping(ctx, nil)
	if err != nil {
		_ = conn.Disconnect(ctx)
		return nil, nil, errors.Wrap(err, "ping")
	}

	return conn, mopts, nil
}

func validateConnStringOpts(opts *options.ClientOptions) error {
	var err error
	if err = opts.Validate(); err != nil {
		return err
	}
	if err = validateWriteConcern(opts.WriteConcern); err != nil {
		return err
	}
	if err = validateReadConcern(opts.ReadConcern); err != nil {
		return err
	}
	if opts.ReadConcern.Level == readconcern.Majority().Level &&
		opts.WriteConcern.W == writeconcern.W1().W {
		return errors.New("ReadConcern majority and WriteConcern 1 is not allowed")
	}

	return nil
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

func UnsafeClient(m *mongo.Client) *clientImpl {
	return &clientImpl{
		client:  m,
		options: options.Client(),
	}
}

// Connect resolves MongoDB connection to Primary member and wraps it within Client object.
// In case of replica set it returns connection to Primary member,
// while in case of sharded cluster it returns connection to Config RS Primary member.
func Connect(ctx context.Context, uri, appName string) (*clientImpl, error) {
	client, opts, err := MongoConnectWithOpts(ctx, uri, AppName(appName))
	if err != nil {
		return nil, errors.Wrap(err, "create mongo connection")
	}

	inf, err := getNodeInfo(ctx, client)
	if err != nil {
		_ = client.Disconnect(ctx)
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
		_ = client.Disconnect(ctx)
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
		_ = client.Disconnect(ctx)
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
		return nil, errors.Wrap(err, "parse mongo-uri")
	}

	// Preserving the `replicaSet` parameter will cause an error
	// while connecting to the ConfigServer (mismatched replicaset names)
	curi.Host = chost[1]
	client, err = MongoConnect(ctx, curi.String(), AppName(appName), NoRS())
	if err != nil {
		return nil, errors.Wrap(err, "create mongo connection to configsvr")
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

// Disconnect tears down a raw mongo client. It is a no-op for a nil client, so it
// is safe to defer unconditionally over an optional connection.
func Disconnect(client *mongo.Client) {
	if client == nil {
		return
	}
	if err := client.Disconnect(context.Background()); err != nil {
		log.Printf("disconnect mongo client: %v", err)
	}
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

func (l *clientImpl) AdminCommand(
	ctx context.Context,
	cmd bson.D,
	opts ...options.Lister[options.RunCmdOptions],
) *mongo.SingleResult {
	cmd = l.applyOptonsFromConnString(cmd)
	return l.client.Database(defs.DB).RunCommand(ctx, cmd, opts...)
}

func (l *clientImpl) applyOptonsFromConnString(cmd bson.D) bson.D {
	if len(cmd) == 0 {
		return cmd
	}

	cmdName := cmd[0].Key
	switch cmdName {
	case "create":
		if wc := l.options.WriteConcern; wc != nil && wc.W != nil {
			cmd = append(cmd, bson.E{
				Key:   "writeConcern",
				Value: bson.D{{Key: "w", Value: wc.W}},
			})
		}
	default:
		// do nothing for all other commands:
		// flushRouterConfig
		// _configsvrBalancerStart
		// _configsvrBalancerStop
		// _configsvrBalancerStatus
	}

	return cmd
}

// nodeInfo represents the mongo's node info
type nodeInfo struct {
	Msg               string `bson:"msg"`
	Me                string `bson:"me"`
	SetName           string `bson:"setName,omitempty"`
	Primary           string `bson:"primary,omitempty"`
	IsPrimary         bool   `bson:"isWritablePrimary"`
	ConfigSvr         int    `bson:"configsvr,omitempty"`
	ConfigServerState *struct {
		OpTime *struct {
			TS   bson.Timestamp `bson:"ts" json:"ts"`
			Term int64          `bson:"t" json:"t"`
		} `bson:"opTime"`
	} `bson:"$configServerState,omitempty"`
	Opts *mongodOpts `bson:"-"`
}

// isSharded returns true is replset is part sharded cluster
func (i *nodeInfo) isSharded() bool {
	return i.SetName != "" && (i.ConfigServerState != nil || i.Opts.Sharding.ClusterRole != "" || i.isConfigsvr())
}

// isConfigsvr returns replset role in sharded clister
func (i *nodeInfo) isConfigsvr() bool {
	return i.ConfigSvr == 2
}

// IsSharded returns true is replset is part sharded cluster
func (i *nodeInfo) isMongos() bool {
	return i.Msg == "isdbgrid"
}

// IsLeader returns true if node can act as backup leader (it's configsrv or non shareded rs)
func (i *nodeInfo) isLeader() bool {
	return !i.isSharded() || i.isConfigsvr()
}

func (i *nodeInfo) isClusterLeader() bool {
	return i.IsPrimary && i.Me == i.Primary && i.isLeader()
}

type mongodOpts struct {
	Sharding struct {
		ClusterRole string `bson:"clusterRole" json:"clusterRole" yaml:"-"`
	} `bson:"sharding" json:"sharding" yaml:"-"`
}

func getNodeInfo(ctx context.Context, m *mongo.Client) (*nodeInfo, error) {
	res := m.Database(defs.DB).RunCommand(ctx, bson.D{{"hello", 1}})
	if err := res.Err(); err != nil {
		return nil, errors.Wrap(err, "cmd: hello")
	}

	n := &nodeInfo{}
	err := res.Decode(&n)
	return n, errors.Wrap(err, "decode")
}

func getMongodOpts(ctx context.Context, m *mongo.Client, defaults *mongodOpts) (*mongodOpts, error) {
	opts := struct {
		Parsed mongodOpts `bson:"parsed" json:"parsed"`
	}{}
	if defaults != nil {
		opts.Parsed = *defaults
	}
	err := m.Database("admin").RunCommand(ctx, bson.D{{"getCmdLineOpts", 1}}).Decode(&opts)
	if err != nil {
		return nil, errors.Wrap(err, "run mongo command")
	}
	return &opts.Parsed, nil
}

func getConfigsvrURI(ctx context.Context, cn *mongo.Client) (string, error) {
	csvr := struct {
		URI string `bson:"configsvrConnectionString"`
	}{}
	err := cn.Database("admin").Collection("system.version").
		FindOne(ctx, bson.D{{"_id", "shardIdentity"}}).Decode(&csvr)

	return csvr.URI, err
}

var ErrInvalidConnection = errors.New("invalid mongo connection")

type Client interface {
	Disconnect(ctx context.Context) error

	MongoClient() *mongo.Client
	MongoOptions() *options.ClientOptions

	ConfigDatabase() *mongo.Database
	AdminCommand(ctx context.Context, cmd bson.D, opts ...options.Lister[options.RunCmdOptions]) *mongo.SingleResult

	LogCollection() *mongo.Collection
	ConfigCollection() *mongo.Collection
	LockCollection() *mongo.Collection
	LockOpCollection() *mongo.Collection
	BcpCollection() *mongo.Collection
	RestoresCollection() *mongo.Collection
	CmdStreamCollection() *mongo.Collection
	PITRChunksCollection() *mongo.Collection
	PITRCollection() *mongo.Collection
	PBMOpLogCollection() *mongo.Collection
	AgentsStatusCollection() *mongo.Collection
}
