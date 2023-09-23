package pbm

import (
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/internal/connect"
	"github.com/percona/percona-backup-mongodb/internal/context"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/lock"
	"github.com/percona/percona-backup-mongodb/internal/log"
	"github.com/percona/percona-backup-mongodb/internal/query"
	"github.com/percona/percona-backup-mongodb/internal/topo"
	"github.com/percona/percona-backup-mongodb/internal/types"
)

type PBM struct {
	Conn connect.MetaClient
	log  *log.Logger
}

// New creates a new PBM object.
// In the sharded cluster both agents and ctls should have a connection to ConfigServer replica set
// in order to communicate via PBM collections.
// If agent's or ctl's local node is not a member of ConfigServer,
// after discovering current topology connection will be established to ConfigServer.
func New(ctx context.Context, uri, appName string) (*PBM, error) {
	c, err := connect.Connect(ctx, uri, &connect.ConnectOptions{AppName: appName})
	if err != nil {
		return nil, errors.Wrap(err, "create mongo connection")
	}

	pbm := &PBM{Conn: c}
	return pbm, errors.Wrap(query.SetupNewDB(ctx, c), "setup a new backups db")
}

func (p *PBM) InitLogger(rs, node string) {
	p.log = log.New(p.Conn.LogCollection(), rs, node)
}

func (p *PBM) Logger() *log.Logger {
	return p.log
}

// GetShards gets list of shards
func (p *PBM) GetShards(ctx context.Context) ([]topo.Shard, error) {
	cur, err := p.Conn.ConfigDatabase().Collection("shards").Find(ctx, bson.M{})
	if err != nil {
		return nil, errors.Wrap(err, "query mongo")
	}
	defer cur.Close(ctx)

	shards := []topo.Shard{}
	for cur.Next(ctx) {
		s := topo.Shard{}
		err := cur.Decode(&s)
		if err != nil {
			return nil, errors.Wrap(err, "message decode")
		}
		s.RS = s.ID
		// _id may differ from the rs name, so extract rs name from the host (format like "rs2/localhost:27017")
		// see https://jira.percona.com/browse/PBM-595
		h := strings.Split(s.Host, "/")
		if len(h) > 1 {
			s.RS = h[0]
		}
		shards = append(shards, s)
	}

	return shards, cur.Err()
}

// SetBalancerStatus sets balancer status
func (p *PBM) SetBalancerStatus(ctx context.Context, m defs.BalancerMode) error {
	var cmd string

	switch m {
	case defs.BalancerModeOn:
		cmd = "_configsvrBalancerStart"
	case defs.BalancerModeOff:
		cmd = "_configsvrBalancerStop"
	default:
		return errors.Errorf("unknown mode %s", m)
	}

	err := p.Conn.AdminCommand(ctx, bson.D{{cmd, 1}}).Err()
	if err != nil {
		return errors.Wrap(err, "run mongo command")
	}
	return nil
}

// GetBalancerStatus returns balancer status
func (p *PBM) GetBalancerStatus(ctx context.Context) (*types.BalancerStatus, error) {
	inf := &types.BalancerStatus{}
	err := p.Conn.AdminCommand(ctx, bson.D{{"_configsvrBalancerStatus", 1}}).Decode(inf)
	if err != nil {
		return nil, errors.Wrap(err, "run mongo command")
	}
	return inf, nil
}

func BackupCursorName(s string) string {
	return strings.NewReplacer("-", "", ":", "").Replace(s)
}

// PITRrun checks if PITR slicing is running. It looks for PITR locks
// and returns true if there is at least one not stale.
func (p *PBM) PITRrun(ctx context.Context) (bool, error) {
	l, err := lock.GetLocks(ctx, p.Conn, &lock.LockHeader{Type: defs.CmdPITR})
	if errors.Is(err, mongo.ErrNoDocuments) || len(l) == 0 {
		return false, nil
	}

	if err != nil {
		return false, errors.Wrap(err, "get locks")
	}

	ct, err := topo.GetClusterTime(ctx, p.Conn)
	if err != nil {
		return false, errors.Wrap(err, "get cluster time")
	}

	for _, lk := range l {
		if lk.Heartbeat.T+defs.StaleFrameSec >= ct.T {
			return true, nil
		}
	}

	return false, nil
}
