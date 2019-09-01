package pbm

import (
	"context"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Node struct {
	name string
	ctx  context.Context
	cn   *mongo.Client
	curi string
}

func NewNode(ctx context.Context, name string, conn *mongo.Client, curi string) *Node {
	return &Node{
		name: name,
		ctx:  ctx,
		cn:   conn,
		curi: curi,
	}
}

func (n *Node) GetIsMaster() (*IsMaster, error) {
	im := &IsMaster{}
	err := n.cn.Database(DB).RunCommand(n.ctx, bson.D{{"isMaster", 1}}).Decode(im)
	if err != nil {
		return nil, errors.Wrap(err, "run mongo command isMaster")
	}
	return im, nil
}

func (n *Node) Name() (string, error) {
	im, err := n.GetIsMaster()
	if err != nil {
		return "", err
	}
	return im.Me, nil
}

type MongoVersion struct {
	VersionString string `bson:"version"`
	Version       []int  `bson:"versionArray"`
}

func (n *Node) GetMongoVersion() (*MongoVersion, error) {
	ver := new(MongoVersion)
	err := n.cn.Database(DB).RunCommand(n.ctx, bson.D{{"buildInfo", 1}}).Decode(ver)
	return ver, err
}

func (n *Node) GetReplsetStatus() (*ReplsetStatus, error) {
	status := &ReplsetStatus{}
	err := n.cn.Database(DB).RunCommand(n.ctx, bson.D{{"replSetGetStatus", 1}}).Decode(status)
	if err != nil {
		return nil, errors.Wrap(err, "run mongo command replSetGetStatus")
	}
	return status, err
}

func (n *Node) Status() (*NodeStatus, error) {
	s, err := n.GetReplsetStatus()
	if err != nil {
		return nil, err
	}

	name, err := n.Name()
	if err != nil {
		return nil, errors.Wrap(err, "get node name")
	}

	for _, m := range s.Members {
		if m.Name == name {
			return &m, nil
		}
	}

	return nil, errors.New("not found")
}

func (n *Node) ConnURI() string {
	return n.curi
}

func (n *Node) Session() *mongo.Client {
	return n.cn
}

func (n *Node) Reconnect() error {
	err := n.cn.Disconnect(n.ctx)
	if err != nil {
		return errors.Wrap(err, "disconnect")
	}
	n.cn = nil
	n.cn, err = mongo.NewClient(options.Client().ApplyURI(n.curi))
	if err != nil {
		return errors.Wrap(err, "new mongo client")
	}
	err = n.cn.Connect(n.ctx)
	if err != nil {
		return errors.Wrap(err, "mongo connect")
	}

	return errors.Wrap(n.cn.Ping(n.ctx, nil), "mongo ping")
}
