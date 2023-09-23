package types

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"

	"github.com/mongodb/mongo-tools/common/db"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/storage/s3"
)

type RestoreMeta struct {
	Status           defs.Status         `bson:"status" json:"status"`
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
	Type             defs.BackupType     `bson:"type" json:"type"`
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
	Status           defs.Status         `bson:"status" json:"status"`
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
	Status           defs.Status         `bson:"status" json:"status"`
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
