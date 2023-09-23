package pbm

import (
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona/percona-backup-mongodb/internal/context"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/topo"
	"github.com/percona/percona-backup-mongodb/internal/types"
)

type Node struct {
	rs        string
	me        string
	cn        *mongo.Client
	curi      string
	dumpConns int
}

func NewNode(ctx context.Context, curi string, dumpConns int) (*Node, error) {
	n := &Node{
		curi:      curi,
		dumpConns: dumpConns,
	}
	err := n.Connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "connect")
	}

	nodeInfo, err := topo.GetNodeInfoExt(ctx, n.Session())
	if err != nil {
		return nil, errors.Wrap(err, "get node info")
	}
	n.rs, n.me = nodeInfo.SetName, nodeInfo.Me

	return n, nil
}

// ID returns node ID
func (n *Node) ID() string {
	return fmt.Sprintf("%s/%s", n.rs, n.me)
}

// RS return replicaset name node belongs to
func (n *Node) RS() string {
	return n.rs
}

// Name returns node name
func (n *Node) Name() string {
	return n.me
}

func (n *Node) Connect(ctx context.Context) error {
	conn, err := n.connect(ctx, true)
	if err != nil {
		return err
	}

	if n.cn != nil {
		err = n.cn.Disconnect(ctx)
		if err != nil {
			return errors.Wrap(err, "close existing connection")
		}
	}

	n.cn = conn
	return nil
}

func (n *Node) connect(ctx context.Context, direct bool) (*mongo.Client, error) {
	opts := options.Client().ApplyURI(n.curi).
		SetAppName("pbm-agent-exec").
		SetDirect(direct)
	conn, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, errors.Wrap(err, "connect")
	}

	err = conn.Ping(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "ping")
	}

	return conn, nil
}

// DBSize returns the total size in bytes of a specific db files on disk on replicaset.
// If db is empty string, returns total size for all databases.
func (n *Node) DBSize(ctx context.Context, db string) (int64, error) {
	q := bson.D{}
	if db != "" {
		q = append(q, bson.E{"name", db})
	}

	res, err := n.cn.ListDatabases(ctx, q)
	if err != nil {
		return 0, errors.Wrap(err, "run mongo command listDatabases")
	}

	return res.TotalSize, nil
}

// IsSharded return true if node is part of the sharded cluster (in shard or configsrv replset).
func (n *Node) IsSharded(ctx context.Context) (bool, error) {
	i, err := topo.GetNodeInfoExt(ctx, n.Session())
	if err != nil {
		return false, err
	}

	return i.IsSharded(), nil
}

func (n *Node) Status(ctx context.Context) (*topo.NodeStatus, error) {
	s, err := topo.GetReplsetStatus(ctx, n.Session())
	if err != nil {
		return nil, errors.Wrap(err, "get replset status")
	}

	name := n.Name()

	for _, m := range s.Members {
		if m.Name == name {
			return &m, nil
		}
	}

	return nil, errors.ErrNotFound
}

// ReplicationLag returns node replication lag in seconds
func (n *Node) ReplicationLag(ctx context.Context) (int, error) {
	return topo.ReplicationLag(ctx, n.Session(), n.Name())
}

func (n *Node) ConnURI() string {
	return n.curi
}

func (n *Node) DumpConns() int {
	return n.dumpConns
}

func (n *Node) Session() *mongo.Client {
	return n.cn
}

func (n *Node) CurrentUser(ctx context.Context) (*types.AuthInfo, error) {
	c := &types.ConnectionStatus{}
	err := n.cn.Database(defs.DB).RunCommand(ctx, bson.D{{"connectionStatus", 1}}).Decode(c)
	if err != nil {
		return nil, errors.Wrap(err, "run mongo command connectionStatus")
	}

	return &c.AuthInfo, nil
}

func (n *Node) DropTMPcoll(ctx context.Context) error {
	cn, err := n.connect(ctx, false)
	if err != nil {
		return errors.Wrap(err, "connect to primary")
	}
	defer cn.Disconnect(ctx) //nolint:errcheck

	err = DropTMPcoll(ctx, cn)
	if err != nil {
		return err
	}

	return nil
}

func DropTMPcoll(ctx context.Context, cn *mongo.Client) error {
	err := cn.Database(defs.DB).Collection(defs.TmpRolesCollection).Drop(ctx)
	if err != nil {
		return errors.Wrapf(err, "drop collection %s", defs.TmpRolesCollection)
	}

	err = cn.Database(defs.DB).Collection(defs.TmpUsersCollection).Drop(ctx)
	if err != nil {
		return errors.Wrapf(err, "drop collection %s", defs.TmpUsersCollection)
	}

	return nil
}

func (n *Node) WaitForWrite(ctx context.Context, ts primitive.Timestamp) error {
	var lw primitive.Timestamp
	var err error

	for i := 0; i < 21; i++ {
		lw, err = topo.GetLastWrite(ctx, n.Session(), false)
		if err == nil && lw.Compare(ts) >= 0 {
			return nil
		}
		time.Sleep(time.Second * 1)
	}

	if err != nil {
		return err
	}

	return errors.New("run out of time")
}

//nolint:nonamedreturns
func (n *Node) CopyUsersNRolles(ctx context.Context) (lastWrite primitive.Timestamp, err error) {
	cn, err := n.connect(ctx, false)
	if err != nil {
		return lastWrite, errors.Wrap(err, "connect to primary")
	}
	defer cn.Disconnect(ctx) //nolint:errcheck

	err = DropTMPcoll(ctx, cn)
	if err != nil {
		return lastWrite, errors.Wrap(err, "drop tmp collections before copy")
	}

	_, err = copyColl(ctx,
		cn.Database("admin").Collection("system.roles"),
		cn.Database(defs.DB).Collection(defs.TmpRolesCollection),
		bson.M{},
	)
	if err != nil {
		return lastWrite, errors.Wrap(err, "copy admin.system.roles")
	}
	_, err = copyColl(ctx,
		cn.Database("admin").Collection("system.users"),
		cn.Database(defs.DB).Collection(defs.TmpUsersCollection),
		bson.M{},
	)
	if err != nil {
		return lastWrite, errors.Wrap(err, "copy admin.system.users")
	}

	return topo.GetLastWrite(ctx, cn, false)
}

// copyColl copy documents matching the given filter and return number of copied documents
func copyColl(ctx context.Context, from, to *mongo.Collection, filter any) (int, error) {
	cur, err := from.Find(ctx, filter)
	if err != nil {
		return 0, errors.Wrap(err, "create cursor")
	}
	defer cur.Close(ctx)

	n := 0
	for cur.Next(ctx) {
		_, err = to.InsertOne(ctx, cur.Current)
		if err != nil {
			return 0, errors.Wrap(err, "insert document")
		}
		n++
	}

	return n, nil
}

func (n *Node) Shutdown(ctx context.Context) error {
	err := n.cn.Database("admin").RunCommand(ctx, bson.D{{"shutdown", 1}}).Err()
	if err == nil || strings.Contains(err.Error(), "socket was unexpectedly closed") {
		return nil
	}
	return err
}
