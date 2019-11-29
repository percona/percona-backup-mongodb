package pbm

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
)

const (
	// DB is a name of the PBM database
	DB = "admin"
	// LogCollection is the name of the mongo collection that contains PBM logs
	LogCollection = "pbmLog"
	// ConfigCollection is the name of the mongo collection that contains PBM configs
	ConfigCollection = "pbmConfig"
	// LockCollection is the name of the mongo collection that is used
	// by agents to coordinate operations (e.g. locks)
	LockCollection = "pbmLock"
	// BcpCollection is a collection for backups metadata
	BcpCollection = "pbmBackups"
	// CmdStreamCollection is the name of the mongo collection that contains backup/restore commands stream
	CmdStreamCollection = "pbmCmd"
)

const (
	// NoReplset is the name of a virtual replica set of the standalone node
	NoReplset = "pbmnoreplicaset"
)

type Command string

const (
	CmdUndefined Command = ""
	CmdBackup            = "backup"
	CmdRestore           = "restore"
)

type Cmd struct {
	Cmd     Command    `bson:"cmd"`
	Backup  BackupCmd  `bson:"backup,omitempty"`
	Restore RestoreCmd `bson:"restore,omitempty"`
	TS      int64      `bson:"ts"`
}

type BackupCmd struct {
	Name        string          `bson:"name"`
	Compression CompressionType `bson:"compression"`
	StoreName   string          `bson:"store,omitempty"`
}

type RestoreCmd struct {
	BackupName string `bson:"backupName"`
	StoreName  string `bson:"store,omitempty"`
}

type CompressionType string

const (
	CompressionTypeNone   CompressionType = "none"
	CompressionTypeGZIP                   = "gzip"
	CompressionTypeSNAPPY                 = "snappy"
	CompressionTypeLZ4                    = "lz4"
)

type PBM struct {
	Conn *mongo.Client
	ctx  context.Context
}

// New creates a new PBM object.
// In the sharded cluster both agents and ctls should have a connection to ConfigServer replica set in order to communicate via PBM collections.
// If agent's or ctl's local node is not a member of CongigServer, after discovering current topology connection will be established to ConfigServer.
func New(ctx context.Context, lh_uri_str, appName string) (*PBM, error) {
	lh_uri, err := url.Parse(lh_uri_str)
	if err != nil || lh_uri.Scheme != "mongodb" {
		return nil, errors.Wrap(err, "The mongo-uri value was not a valid mongodb://..../ connection URI.")
	}

	client, err := connect(ctx, lh_uri.String(), "pbm-discovery")
	if err != nil {
		return nil, errors.Wrap(err, "create mongo connection")
	}

	pbm := &PBM{
		Conn: client,
		ctx:  ctx,
	}
	im, err := pbm.GetIsMaster()
	if err != nil {
		return nil, errors.Wrap(err, "get topology")
	}

	if !im.IsSharded() || im.ReplsetRole() == ReplRoleConfigSrv {

		//If the "replicaSet" was (unexpectedly) set in lh_uri we already
		//  have an rs-style connection, so return now
		if _, k_found := lh_uri.Query()["replicaSet"]; k_found {
			return pbm, nil
		}

		//Recreate connection as rs-style one, to read from primary
		client.Disconnect(ctx)
		rs_uri := lh_uri
		SetReplicaSet(rs_uri, im.SetName)
		pbm.Conn, err = connect(ctx, rs_uri.String(), appName)
		if err != nil {
			return nil, errors.Wrap(err, "could not create mongo connection to " + rs_uri.String())
		}
		return pbm, nil
	}

	cfg_rs_nm, cfg_hostport_list, err := ConfigServerRsNameAndHostList(client, ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get config server connetion URI")
	}
	// no need in this connection anymore, we need a new one with the ConfigServer
	client.Disconnect(ctx)

	//Make a new URI to connect to the configsvr replicaset. Set the hosts in the Host
	//  part, add replicaSet in the Query part. Reuse all other parts of the URI as-is.
	cfgrs_uri := lh_uri
	cfgrs_uri.Host = cfg_hostport_list
	SetReplicaSet(cfgrs_uri, cfg_rs_nm)

	pbm.Conn, err = connect(ctx, cfgrs_uri.String(), appName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create mongo connection to configsvr replicaset. " +
					"Calculated connection string was: " + cfgrs_uri.String())
	}

	return pbm, errors.Wrap(pbm.setupNewDB(), "setup a new backups db")
}

// setup a new DB for PBM
func (p *PBM) setupNewDB() error {
	err := p.Conn.Database(DB).RunCommand(
		p.ctx,
		bson.D{{"create", CmdStreamCollection}, {"capped", true}, {"size", 1 << 10 * 2}},
	).Err()
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return errors.Wrap(err, "ensure cmd collection")
	}

	err = p.Conn.Database(DB).RunCommand(
		p.ctx,
		bson.D{{"create", LockCollection}}, //size 2kb ~ 10 commands
	).Err()
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return errors.Wrap(err, "ensure lock collection")
	}

	// create index for Locks
	c := p.Conn.Database(DB).Collection(LockCollection)
	_, err = c.Indexes().CreateOne(
		p.ctx,
		mongo.IndexModel{
			Keys: bson.D{{"replset", 1}},
			Options: options.Index().
				SetUnique(true).
				SetSparse(true),
		},
	)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return errors.Wrap(err, "ensure lock index")
	}

	return nil
}

func connect(ctx context.Context, uri, appName string) (*mongo.Client, error) {
	client, err := mongo.NewClient(
		options.Client().ApplyURI(uri).
			SetAppName(appName).
			SetReadPreference(readpref.Primary()).
			SetReadConcern(readconcern.Majority()).
			SetWriteConcern(writeconcern.New(writeconcern.WMajority())),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create mongo client")
	}
	err = client.Connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "mongo connect")
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "mongo ping")
	}

	return client, nil
}

// BackupMeta is a backup's metadata
type BackupMeta struct {
	Name             string              `bson:"name" json:"name"`
	Replsets         []BackupReplset     `bson:"replsets" json:"replsets"`
	Compression      CompressionType     `bson:"compression" json:"compression"`
	Store            Storage             `bson:"store" json:"store"`
	MongoVersion     string              `bson:"mongodb_version" json:"mongodb_version,omitempty"`
	StartTS          int64               `bson:"start_ts" json:"start_ts"`
	LastTransitionTS int64               `bson:"last_transition_ts" json:"last_transition_ts"`
	LastWriteTS      primitive.Timestamp `bson:"last_write_ts" json:"last_write_ts"`
	Hb               primitive.Timestamp `bson:"hb" json:"hb"`
	Status           Status              `bson:"status" json:"status"`
	Conditions       []Condition         `bson:"conditions" json:"conditions"`
	Error            string              `bson:"error,omitempty" json:"error,omitempty"`
}
type Condition struct {
	Timestamp int64  `bson:"timestamp" json:"timestamp"`
	Status    Status `bson:"status" json:"status"`
	Error     string `bson:"error,omitempty" json:"error,omitempty"`
}

type BackupReplset struct {
	Name             string              `bson:"name" json:"name"`
	DumpName         string              `bson:"dump_name" json:"backup_name" `
	OplogName        string              `bson:"oplog_name" json:"oplog_name"`
	StartTS          int64               `bson:"start_ts" json:"start_ts"`
	Status           Status              `bson:"status" json:"status"`
	LastTransitionTS int64               `bson:"last_transition_ts" json:"last_transition_ts"`
	LastWriteTS      primitive.Timestamp `bson:"last_write_ts" json:"last_write_ts"`
	Error            string              `bson:"error,omitempty" json:"error,omitempty"`
	Conditions       []Condition         `bson:"conditions" json:"conditions"`
}

// Status is backup current status
type Status string

const (
	StatusStarting Status = "starting"
	StatusRunning         = "running"
	StatusDumpDone        = "dumpDone"
	StatusDone            = "done"
	StatusError           = "error"
)

func (p *PBM) SetBackupMeta(m *BackupMeta) error {
	m.LastTransitionTS = m.StartTS
	m.Conditions = append(m.Conditions, Condition{
		Timestamp: m.StartTS,
		Status:    m.Status,
	})

	_, err := p.Conn.Database(DB).Collection(BcpCollection).InsertOne(p.ctx, m)

	return err
}

func (p *PBM) ChangeBackupState(bcpName string, s Status, msg string) error {
	ts := time.Now().UTC().Unix()
	_, err := p.Conn.Database(DB).Collection(BcpCollection).UpdateOne(
		p.ctx,
		bson.D{{"name", bcpName}},
		bson.D{
			{"$set", bson.M{"status": s}},
			{"$set", bson.M{"last_transition_ts": ts}},
			{"$set", bson.M{"error": msg}},
			{"$push", bson.M{"conditions": Condition{Timestamp: ts, Status: s, Error: msg}}},
		},
	)

	return err
}

func (p *PBM) SetBackupHB(bcpName string, ts primitive.Timestamp) error {
	_, err := p.Conn.Database(DB).Collection(BcpCollection).UpdateOne(
		p.ctx,
		bson.D{{"name", bcpName}},
		bson.D{
			{"$set", bson.M{"hb": ts}},
		},
	)

	return err
}

func (p *PBM) SetLastWrite(bcpName string, ts primitive.Timestamp) error {
	_, err := p.Conn.Database(DB).Collection(BcpCollection).UpdateOne(
		p.ctx,
		bson.D{{"name", bcpName}},
		bson.D{
			{"$set", bson.M{"last_write_ts": ts}},
		},
	)

	return err
}

func (p *PBM) AddRSMeta(bcpName string, rs BackupReplset) error {
	rs.LastTransitionTS = rs.StartTS
	rs.Conditions = append(rs.Conditions, Condition{
		Timestamp: rs.StartTS,
		Status:    rs.Status,
	})
	_, err := p.Conn.Database(DB).Collection(BcpCollection).UpdateOne(
		p.ctx,
		bson.D{{"name", bcpName}},
		bson.D{{"$addToSet", bson.M{"replsets": rs}}},
	)

	return err
}

func (p *PBM) ChangeRSState(bcpName string, rsName string, s Status, msg string) error {
	ts := time.Now().UTC().Unix()
	_, err := p.Conn.Database(DB).Collection(BcpCollection).UpdateOne(
		p.ctx,
		bson.D{{"name", bcpName}, {"replsets.name", rsName}},
		bson.D{
			{"$set", bson.M{"replsets.$.status": s}},
			{"$set", bson.M{"replsets.$.last_transition_ts": ts}},
			{"$set", bson.M{"replsets.$.error": msg}},
			{"$push", bson.M{"replsets.$.conditions": Condition{Timestamp: ts, Status: s, Error: msg}}},
		},
	)

	return err
}

func (p *PBM) SetRSLastWrite(bcpName string, rsName string, ts primitive.Timestamp) error {
	_, err := p.Conn.Database(DB).Collection(BcpCollection).UpdateOne(
		p.ctx,
		bson.D{{"name", bcpName}, {"replsets.name", rsName}},
		bson.D{
			{"$set", bson.M{"replsets.$.last_write_ts": ts}},
		},
	)

	return err
}

func (p *PBM) GetBackupMeta(name string) (*BackupMeta, error) {
	b := new(BackupMeta)
	res := p.Conn.Database(DB).Collection(BcpCollection).FindOne(p.ctx, bson.D{{"name", name}})
	if res.Err() != nil {
		if res.Err() == mongo.ErrNoDocuments {
			return b, nil
		}
		return nil, errors.Wrap(res.Err(), "get")
	}
	err := res.Decode(b)
	return b, errors.Wrap(err, "decode")
}

func (p *PBM) BackupsList(limit int64) ([]BackupMeta, error) {
	cur, err := p.Conn.Database(DB).Collection(BcpCollection).Find(
		p.ctx,
		bson.M{},
		options.Find().SetLimit(limit).SetSort(bson.D{{"start_ts", 1}}),
	)
	if err != nil {
		return nil, errors.Wrap(err, "query mongo")
	}

	defer cur.Close(p.ctx)

	backups := []BackupMeta{}
	for cur.Next(p.ctx) {
		b := BackupMeta{}
		err := cur.Decode(&b)
		if err != nil {
			return nil, errors.Wrap(err, "message decode")
		}
		backups = append(backups, b)
	}

	return backups, cur.Err()
}

// GetShards gets list of shards
func (p *PBM) GetShards() ([]Shard, error) {
	cur, err := p.Conn.Database("config").Collection("shards").Find(p.ctx, bson.M{})
	if err != nil {
		return nil, errors.Wrap(err, "query mongo")
	}

	defer cur.Close(p.ctx)

	shards := []Shard{}
	for cur.Next(p.ctx) {
		s := Shard{}
		err := cur.Decode(&s)
		if err != nil {
			return nil, errors.Wrap(err, "message decode")
		}
		shards = append(shards, s)
	}

	return shards, cur.Err()
}

// Context returns object context
func (p *PBM) Context() context.Context {
	return p.ctx
}

// GetIsMaster returns IsMaster object encapsulating respective MongoDB structure
func (p *PBM) GetIsMaster() (*IsMaster, error) {
	im := &IsMaster{}
	err := p.Conn.Database(DB).RunCommand(p.ctx, bson.D{{"isMaster", 1}}).Decode(im)
	if err != nil {
		return nil, errors.Wrap(err, "run mongo command isMaster")
	}
	return im, nil
}

// ClusterTime returns mongo's current cluster time
func (p *PBM) ClusterTime() (primitive.Timestamp, error) {
	// Make a read to force the cluster timestamp update.
	// Otherwise, cluster timestamp could remain the same between `isMaster` reads, while in fact time has been moved forward.
	err := p.Conn.Database(DB).Collection(LockCollection).FindOne(p.ctx, bson.D{}).Err()
	if err != nil && err != mongo.ErrNoDocuments {
		return primitive.Timestamp{}, errors.Wrap(err, "void read")
	}

	im, err := p.GetIsMaster()
	if err != nil {
		return primitive.Timestamp{}, errors.Wrap(err, "get isMaster")
	}

	if im.ClusterTime == nil {
		return primitive.Timestamp{}, errors.Wrap(err, "no clusterTime in response")
	}

	return im.ClusterTime.ClusterTime, nil
}

// Replace (or set for the first time) the "replicaSet" query value in a URI
func SetReplicaSet(uri *url.URL, new_rs string) {
        query := uri.Query()
        query.Set("replicaSet", new_rs)
        uri.RawQuery = query.Encode()
}

//Return configsvr replicaset name and host:port list from
//  admin.system.version.findOne({"_id", "shardIdentity"}).configsvrConnectionString
func ConfigServerRsNameAndHostList(client *mongo.Client, ctx context.Context) (string, string, error) {
        csvr := struct {
                //string will have "rsname/host:port[,host:port]*" format
                rsHostlistStr string `bson:"configsvrConnectionString"`
        }{}
        err := client.Database("admin").Collection("system.version").
                FindOne(ctx, bson.D{{"_id", "shardIdentity"}}).Decode(&csvr)
        if err != nil {
                err_str := "system.version.findOne({\"_id\", \"shardIdentity\"}) not found in \"admin\" db."
                return "", "", errors.Wrapf(err, err_str)
        }
        a := strings.Split(csvr.rsHostlistStr, "/")
        if len(a) != 2 {
                err_str := "configsvrConnectionString did not have \"rsname/host:port[,host:port]*\" format. " +
                        "Found value was " + csvr.rsHostlistStr
                return "", "", errors.New(err_str)
        }
        return a[0], a[1], nil
}
