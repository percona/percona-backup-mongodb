package pbm

import (
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type Node struct {
	name string
	cn   *mongo.Client
	opts string
}

func NewNode(name string, conn *mongo.Client, curi string) *Node {
	return &Node{
		name: name,
		cn:   conn,
		opts: curi,
	}
}

func (n *Node) GetIsMaster() (*IsMaster, error) {
	im := &IsMaster{}
	err := n.cn.Database(DB).RunCommand(nil, bson.D{{"isMaster", 1}}).Decode(im)
	if err != nil {
		return nil, errors.Wrap(err, "run mongo command")
	}
	return im, nil
}

func (n *Node) ConnURI() string {
	return n.opts
}
