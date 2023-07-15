package pbm

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/mongodb/mongo-tools/common/db"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona/percona-backup-mongodb/pbm/storage/s3"
)

const ExternalRsMetaFile = "pbm.rsmeta.%s.json"

type RestoreMeta struct {
	Status           Status              `bson:"status" json:"status"`
	Error            string              `bson:"error,omitempty" json:"error,omitempty"`
	Name             string              `bson:"name" json:"name"`
	OPID             string              `bson:"opid" json:"opid"`
	Backup           string              `bson:"backup" json:"backup"`
	BcpChain         []string            `bson:"bcp_chain" json:"bcp_chain"` // for incremental
	Namespaces       []string            `bson:"nss,omitempty" json:"nss,omitempty"`
	StartPITR        int64               `bson:"start_pitr" json:"start_pitr"`
	PITR             int64               `bson:"pitr" json:"pitr"`
	Replsets         []RestoreReplset    `bson:"replsets" json:"replsets"`
	Hb               primitive.Timestamp `bson:"hb" json:"hb"`
	StartTS          int64               `bson:"start_ts" json:"start_ts"`
	LastTransitionTS int64               `bson:"last_transition_ts" json:"last_transition_ts"`
	Conditions       Conditions          `bson:"conditions" json:"conditions"`
	Type             BackupType          `bson:"type" json:"type"`
	Leader           string              `bson:"l,omitempty" json:"l,omitempty"`
	Stat             *RestoreStat        `bson:"stat,omitempty" json:"stat,omitempty"`
}

type RestoreStat struct {
	RS map[string]map[string]RestoreRSMetrics `bson:"rs,omitempty" json:"rs,omitempty"`
}
type RestoreRSMetrics struct {
	DistTxn  DistTxnStat     `bson:"txn,omitempty" json:"txn,omitempty"`
	Download s3.DownloadStat `bson:"download,omitempty" json:"download,omitempty"`
}

type DistTxnStat struct {
	// Partial is the num of transactions that were allied on other shards
	// but can't be applied on this one since not all prepare messages got
	// into the oplog (shouldn't happen).
	Partial int `bson:"partial" json:"partial"`
	// ShardUncommitted is the number of uncommitted transactions before
	// the sync. Basically, the transaction is full but no commit message
	// in the oplog of this shard.
	ShardUncommitted int `bson:"shard_uncommitted" json:"shard_uncommitted"`
	// LeftUncommitted is the num of transactions that remain uncommitted
	// after the sync. The transaction is full but no commit message in the
	// oplog of any shard.
	LeftUncommitted int `bson:"left_uncommitted" json:"left_uncommitted"`
}

type RestoreShardStat struct {
	Txn DistTxnStat      `json:"txn"`
	D   *s3.DownloadStat `json:"d"`
}

type RestoreReplset struct {
	Name             string              `bson:"name" json:"name"`
	StartTS          int64               `bson:"start_ts" json:"start_ts"`
	Status           Status              `bson:"status" json:"status"`
	CommittedTxn     []RestoreTxn        `bson:"committed_txn" json:"committed_txn"`
	CommittedTxnSet  bool                `bson:"txn_set" json:"txn_set"`
	PartialTxn       []db.Oplog          `bson:"partial_txn" json:"partial_txn"`
	CurrentOp        primitive.Timestamp `bson:"op" json:"op"`
	LastTransitionTS int64               `bson:"last_transition_ts" json:"last_transition_ts"`
	LastWriteTS      primitive.Timestamp `bson:"last_write_ts" json:"last_write_ts"`
	Nodes            []RestoreNode       `bson:"nodes,omitempty" json:"nodes,omitempty"`
	Error            string              `bson:"error,omitempty" json:"error,omitempty"`
	Conditions       Conditions          `bson:"conditions" json:"conditions"`
	Hb               primitive.Timestamp `bson:"hb" json:"hb"`
	Stat             RestoreShardStat    `bson:"stat" json:"stat"`
}

type Conditions []*Condition

func (b Conditions) Len() int           { return len(b) }
func (b Conditions) Less(i, j int) bool { return b[i].Timestamp < b[j].Timestamp }
func (b Conditions) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

// Insert keeps conditions asc sorted by Timestamp
func (b *Conditions) Insert(c *Condition) {
	i := sort.Search(len(*b), func(i int) bool { return []*Condition(*b)[i].Timestamp >= c.Timestamp })
	*b = append(*b, &Condition{})
	copy([]*Condition(*b)[i+1:], []*Condition(*b)[i:])
	[]*Condition(*b)[i] = c
}

type RestoreNode struct {
	Name             string              `bson:"name" json:"name"`
	Status           Status              `bson:"status" json:"status"`
	LastTransitionTS int64               `bson:"last_transition_ts" json:"last_transition_ts"`
	Error            string              `bson:"error,omitempty" json:"error,omitempty"`
	Conditions       Conditions          `bson:"conditions" json:"conditions"`
	Hb               primitive.Timestamp `bson:"hb" json:"hb"`
}

type TxnState string

const (
	TxnCommit  TxnState = "commit"
	TxnPrepare TxnState = "prepare"
	TxnAbort   TxnState = "abort"
	TxnUnknown TxnState = ""
)

type RestoreTxn struct {
	ID    string              `bson:"id" json:"id"`
	Ctime primitive.Timestamp `bson:"ts" json:"ts"` // commit timestamp of the transaction
	State TxnState            `bson:"state" json:"state"`
}

func (t RestoreTxn) Encode() []byte {
	return []byte(fmt.Sprintf("txn:%d,%d:%s:%s", t.Ctime.T, t.Ctime.I, t.ID, t.State))
}

func (t *RestoreTxn) Decode(b []byte) error {
	for k, v := range bytes.SplitN(bytes.TrimSpace(b), []byte{':'}, 4) {
		switch k {
		case 0:
		case 1:
			if si := bytes.SplitN(v, []byte{','}, 2); len(si) == 2 {
				tt, err := strconv.ParseInt(string(si[0]), 10, 64)
				if err != nil {
					return errors.Wrap(err, "parse clusterTime T")
				}
				ti, err := strconv.ParseInt(string(si[1]), 10, 64)
				if err != nil {
					return errors.Wrap(err, "parse clusterTime I")
				}

				t.Ctime = primitive.Timestamp{T: uint32(tt), I: uint32(ti)}
			}
		case 2:
			t.ID = string(v)
		case 3:
			t.State = TxnState(string(v))
		}
	}

	return nil
}

func (t RestoreTxn) String() string {
	return fmt.Sprintf("<%s> [%s] %v", t.ID, t.State, t.Ctime)
}

func (p *PBM) RestoreSetRSTxn(name, rsName string, txn []RestoreTxn) error {
	_, err := p.Conn.Database(DB).Collection(RestoresCollection).UpdateOne(
		p.ctx,
		bson.D{{"name", name}, {"replsets.name", rsName}},
		bson.D{{"$set", bson.M{"replsets.$.committed_txn": txn, "replsets.$.txn_set": true}}},
	)

	return err
}

func (p *PBM) RestoreSetRSStat(name, rsName string, stat RestoreShardStat) error {
	_, err := p.Conn.Database(DB).Collection(RestoresCollection).UpdateOne(
		p.ctx,
		bson.D{{"name", name}, {"replsets.name", rsName}},
		bson.D{{"$set", bson.M{"replsets.$.stat": stat}}},
	)

	return err
}

func (p *PBM) RestoreSetStat(name string, stat RestoreStat) error {
	_, err := p.Conn.Database(DB).Collection(RestoresCollection).UpdateOne(
		p.ctx,
		bson.D{{"name", name}},
		bson.D{{"$set", bson.M{"stat": stat}}},
	)

	return err
}

func (p *PBM) RestoreSetRSPartTxn(name, rsName string, txn []db.Oplog) error {
	_, err := p.Conn.Database(DB).Collection(RestoresCollection).UpdateOne(
		p.ctx,
		bson.D{{"name", name}, {"replsets.name", rsName}},
		bson.D{{"$set", bson.M{"replsets.$.partial_txn": txn}}},
	)

	return err
}

func (p *PBM) SetCurrentOp(name, rsName string, ts primitive.Timestamp) error {
	_, err := p.Conn.Database(DB).Collection(RestoresCollection).UpdateOne(
		p.ctx,
		bson.D{{"name", name}, {"replsets.name", rsName}},
		bson.D{{"$set", bson.M{"replsets.$.op": ts}}},
	)

	return err
}

func (p *PBM) SetRestoreMeta(m *RestoreMeta) error {
	m.LastTransitionTS = m.StartTS
	m.Conditions = append(m.Conditions, &Condition{
		Timestamp: m.StartTS,
		Status:    m.Status,
	})

	_, err := p.Conn.Database(DB).Collection(RestoresCollection).InsertOne(p.ctx, m)

	return err
}

func (p *PBM) GetRestoreMetaByOPID(opid string) (*RestoreMeta, error) {
	return p.getRestoreMeta(bson.D{{"opid", opid}})
}

func (p *PBM) GetRestoreMeta(name string) (*RestoreMeta, error) {
	return p.getRestoreMeta(bson.D{{"name", name}})
}

func (p *PBM) getRestoreMeta(clause bson.D) (*RestoreMeta, error) {
	res := p.Conn.Database(DB).Collection(RestoresCollection).FindOne(p.ctx, clause)
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrNotFound
		}
		return nil, errors.Wrap(err, "get")
	}
	r := &RestoreMeta{}
	err := res.Decode(r)
	return r, errors.Wrap(err, "decode")
}

// GetLastRestore returns last successfully finished restore
// and nil if there is no such restore yet.
func (p *PBM) GetLastRestore() (*RestoreMeta, error) {
	r := new(RestoreMeta)

	res := p.Conn.Database(DB).Collection(RestoresCollection).FindOne(
		p.ctx,
		bson.D{{"status", StatusDone}},
		options.FindOne().SetSort(bson.D{{"start_ts", -1}}),
	)
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrNotFound
		}
		return nil, errors.Wrap(err, "get")
	}
	err := res.Decode(r)
	return r, errors.Wrap(err, "decode")
}

func (p *PBM) AddRestoreRSMeta(name string, rs RestoreReplset) error {
	rs.LastTransitionTS = rs.StartTS
	rs.Conditions = append(rs.Conditions, &Condition{
		Timestamp: rs.StartTS,
		Status:    rs.Status,
	})
	_, err := p.Conn.Database(DB).Collection(RestoresCollection).UpdateOne(
		p.ctx,
		bson.D{{"name", name}},
		bson.D{{"$addToSet", bson.M{"replsets": rs}}},
	)

	return err
}

func (p *PBM) RestoreHB(name string) error {
	ts, err := p.ClusterTime()
	if err != nil {
		return errors.Wrap(err, "read cluster time")
	}

	_, err = p.Conn.Database(DB).Collection(RestoresCollection).UpdateOne(
		p.ctx,
		bson.D{{"name", name}},
		bson.D{
			{"$set", bson.M{"hb": ts}},
		},
	)

	return errors.Wrap(err, "write into db")
}

func (p *PBM) ChangeRestoreStateOPID(opid string, s Status, msg string) error {
	return p.changeRestoreState(bson.D{{"name", opid}}, s, msg)
}

func (p *PBM) ChangeRestoreState(name string, s Status, msg string) error {
	return p.changeRestoreState(bson.D{{"name", name}}, s, msg)
}

func (p *PBM) changeRestoreState(clause bson.D, s Status, msg string) error {
	ts := time.Now().UTC().Unix()
	_, err := p.Conn.Database(DB).Collection(RestoresCollection).UpdateOne(
		p.ctx,
		clause,
		bson.D{
			{"$set", bson.M{"status": s}},
			{"$set", bson.M{"last_transition_ts": ts}},
			{"$set", bson.M{"error": msg}},
			{"$push", bson.M{"conditions": Condition{Timestamp: ts, Status: s, Error: msg}}},
		},
	)

	return err
}

func (p *PBM) SetRestoreBackup(name, backupName string, nss []string) error {
	d := bson.M{"backup": backupName}
	if nss != nil {
		d["nss"] = nss
	}

	_, err := p.Conn.Database(DB).Collection(RestoresCollection).UpdateOne(
		p.ctx,
		bson.D{{"name", name}},
		bson.D{{"$set", d}},
	)

	return err
}

func (p *PBM) SetOplogTimestamps(name string, start, end int64) error {
	_, err := p.Conn.Database(DB).Collection(RestoresCollection).UpdateOne(
		p.ctx,
		bson.M{"name": name},
		bson.M{"$set": bson.M{"start_pitr": start, "pitr": end}},
	)

	return err
}

func (p *PBM) ChangeRestoreRSState(name, rsName string, s Status, msg string) error {
	ts := time.Now().UTC().Unix()
	_, err := p.Conn.Database(DB).Collection(RestoresCollection).UpdateOne(
		p.ctx,
		bson.D{{"name", name}, {"replsets.name", rsName}},
		bson.D{
			{"$set", bson.M{"replsets.$.status": s}},
			{"$set", bson.M{"replsets.$.last_transition_ts": ts}},
			{"$set", bson.M{"replsets.$.error": msg}},
			{"$push", bson.M{"replsets.$.conditions": Condition{Timestamp: ts, Status: s, Error: msg}}},
		},
	)

	return err
}

func (p *PBM) RestoresList(limit int64) ([]RestoreMeta, error) {
	cur, err := p.Conn.Database(DB).Collection(RestoresCollection).Find(
		p.ctx,
		bson.M{},
		options.Find().SetLimit(limit).SetSort(bson.D{{"start_ts", -1}}),
	)
	if err != nil {
		return nil, errors.Wrap(err, "query mongo")
	}
	defer cur.Close(p.ctx)

	restores := []RestoreMeta{}
	for cur.Next(p.ctx) {
		r := RestoreMeta{}
		err := cur.Decode(&r)
		if err != nil {
			return nil, errors.Wrap(err, "message decode")
		}
		restores = append(restores, r)
	}

	return restores, cur.Err()
}
