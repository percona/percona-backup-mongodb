package oplog

import (
	"testing"

	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/idx"
	"github.com/mongodb/mongo-tools/mongorestore/ns"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
	pbmidx "github.com/percona/percona-backup-mongodb/pbm/idx"
	"github.com/percona/percona-backup-mongodb/pbm/version"
)

func TestHandleRenameCollection(t *testing.T) {
	mdb := newMDBTestClient()
	r := newOplogRestoreTest(mdb)
	r.indexCatalog.AddOplogIndex("db", "metrics", &idx.IndexDocument{
		Key: bson.D{{"value", 1}}, Options: bson.M{"name": "source_idx"},
	})
	r.indexCatalog.AddIndex("other", "stats", &idx.IndexDocument{
		Key: bson.D{{"old", 1}}, Options: bson.M{"name": "obsolete"},
	})
	op := renameOplog("db.metrics", "other.stats")

	require.NoError(t, r.handleNonTxnOp(op))

	require.Equal(t, []db.Oplog{op}, mdb.appliedOps)
	require.Empty(t, r.indexCatalog.GetIndexes("db", "metrics"))
	require.NotNil(t, r.indexCatalog.GetIndex("other", "stats", "source_idx"))
	require.Nil(t, r.indexCatalog.GetIndex("other", "stats", "obsolete"))
}

func TestFailedRenamePreservesCatalog(t *testing.T) {
	mdb := newMDBTestClient()
	mdb.applyErr = errors.New("rename rejected")
	r := newOplogRestoreTest(mdb)
	for _, collection := range []string{"metrics", "stats"} {
		r.indexCatalog.AddIndex("db", collection, &idx.IndexDocument{
			Key: bson.D{{"value", 1}}, Options: bson.M{"name": collection + "_idx"},
		})
	}

	err := r.handleNonTxnOp(renameOplog("db.metrics", "db.stats"))

	require.ErrorIs(t, err, mdb.applyErr)
	require.NotNil(t, r.indexCatalog.GetIndex("db", "metrics", "metrics_idx"))
	require.NotNil(t, r.indexCatalog.GetIndex("db", "stats", "stats_idx"))
}

func TestExcludedRenamePreservesCatalog(t *testing.T) {
	mdb := newMDBTestClient()
	r := newOplogRestoreTest(mdb)
	var err error
	r.excludeNS, err = ns.NewMatcher([]string{"db.stats"})
	require.NoError(t, err)
	r.indexCatalog.AddIndex("db", "metrics", &idx.IndexDocument{
		Key: bson.D{{"value", 1}}, Options: bson.M{"name": "source_idx"},
	})
	op := renameOplog("db.metrics", "db.stats")

	require.NoError(t, r.handleNonTxnOp(op))
	require.Empty(t, mdb.appliedOps)
	require.NotNil(t, r.indexCatalog.GetIndex("db", "metrics", "source_idx"))
}

func TestTimeSeriesRenameThenDrop(t *testing.T) {
	const (
		database = "test125_fsmdb0"
		temp     = "system.buckets.tmp.agg_out.9c79fa0c-4512-42ae-97bf-50d958271143"
		output   = "system.buckets.interrupt_temp_out"
	)
	mdb := newMDBTestClient()
	r := newOplogRestoreTest(mdb)
	r.ver = &db.Version{8, 0, 17}
	r.indexCatalog = pbmidx.NewCatalog(&version.MongoVersion{Version: []int{8, 0, 17}})

	// Index-lifecycle commands from PBM-1762's 08:18:30–08:18:36 chunk.
	commands := []struct {
		increment uint32
		command   bson.D
	}{
		{10, bson.D{
			{"create", temp}, {"temp", true}, {"clusteredIndex", true},
			{"timeseries", bson.D{{"timeField", "time"}, {"metaField", "tag"}, {"granularity", "seconds"}}},
		}},
		{15, bson.D{
			{"createIndexes", temp}, {"v", int32(2)}, {"name", "tag_1_time_1"},
			{"key", bson.D{{"meta", int32(1)}, {"control.min.time", int32(1)}, {"control.max.time", int32(1)}}},
		}},
		{21, bson.D{
			{"renameCollection", database + "." + temp}, {"to", database + "." + output}, {"stayTemp", false},
		}},
	}
	for _, command := range commands {
		require.NoError(t, r.handleNonTxnOp(db.Oplog{
			Timestamp: bson.Timestamp{T: 1778141913, I: command.increment},
			Operation: "c", Namespace: database + ".$cmd", Object: command.command,
		}))
	}
	groups := r.indexCatalog.BuildGroups(database, "interrupt_temp_out")
	require.Len(t, groups, 1)
	require.Equal(t, output, groups[0].Collection)
	require.Len(t, groups[0].Indexes, 1)
	require.Equal(t, "tag_1_time_1", groups[0].Indexes[0].Options["name"])
	require.Empty(t, r.indexCatalog.BuildGroups(database, temp))

	require.NoError(t, r.handleNonTxnOp(db.Oplog{
		Timestamp: bson.Timestamp{T: 1778141913, I: 32},
		Operation: "c", Namespace: database + ".$cmd", Object: bson.D{{"drop", output}},
	}))
	require.Empty(t, r.indexCatalog.BuildGroups(database, temp))
	require.Empty(t, r.indexCatalog.BuildGroups(database, output))
	require.Empty(t, r.indexCatalog.Namespaces())
}

func renameOplog(from, to string) db.Oplog {
	sourceUUID := bson.Binary{Subtype: bson.TypeBinaryUUID, Data: make([]byte, 16)}
	targetUUID := bson.Binary{Subtype: bson.TypeBinaryUUID, Data: make([]byte, 16)}
	sourceUUID.Data[0] = 1
	targetUUID.Data[0] = 2
	return db.Oplog{
		Operation: "c",
		Namespace: "admin.$cmd",
		UI:        &sourceUUID,
		Object: bson.D{
			{"renameCollection", from}, {"to", to}, {"dropTarget", targetUUID},
		},
		Query: bson.D{{"numRecords", int64(1)}},
	}
}
