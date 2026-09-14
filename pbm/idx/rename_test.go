package idx_test

import (
	"testing"

	mongoidx "github.com/mongodb/mongo-tools/common/idx"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-backup-mongodb/pbm/idx"
	"github.com/percona/percona-backup-mongodb/pbm/version"
)

func TestCatalogRenameCollection(t *testing.T) {
	tests := []struct {
		name, from, toDB, to, target string
		version                      []int
		rawData                      bool
	}{
		{
			name: "cross database", from: "metrics", toDB: "other", to: "stats",
			target: "stats", version: []int{8, 0},
		},
		{
			name: "legacy 8.0", from: "system.buckets.tmp.agg_out.1", toDB: "db", to: "system.buckets.stats",
			target: "system.buckets.stats", version: []int{8, 0},
		},
		{
			name: "legacy 8.3", from: "system.buckets.tmp.agg_out.1", toDB: "db", to: "system.buckets.stats",
			target: "stats", version: []int{8, 3}, rawData: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalog := idx.NewCatalog(&version.MongoVersion{Version: tt.version})
			catalog.AddOplogIndex("db", tt.from, &mongoidx.IndexDocument{
				Key: bson.D{{"meta.sensor", 1}}, Options: bson.M{"name": "source_idx"},
			})
			catalog.AddIndex(tt.toDB, "stats", &mongoidx.IndexDocument{
				Key: bson.D{{"old", 1}}, Options: bson.M{"name": "old_idx"},
			})

			catalog.RenameCollection("db", tt.from, tt.toDB, tt.to)

			require.Equal(t, []options.Namespace{{DB: tt.toDB, Collection: "stats"}}, catalog.Namespaces())
			require.Empty(t, catalog.GetIndexes("db", tt.from))
			groups := catalog.BuildGroups(tt.toDB, "stats")
			require.Len(t, groups, 1)
			require.Equal(t, tt.target, groups[0].Collection)
			require.Equal(t, tt.rawData, groups[0].RawData)
			require.Len(t, groups[0].Indexes, 1)
			require.Equal(t, "source_idx", groups[0].Indexes[0].Options["name"])
			require.Equal(t, bson.D{{"meta.sensor", 1}}, groups[0].Indexes[0].Key)
		})
	}
}

func TestCatalogRenameCollation(t *testing.T) {
	simpleCollation := bson.D{{"locale", "simple"}}
	englishCollation := bson.D{{"locale", "en"}, {"strength", int32(2)}}
	tests := []struct {
		name                 string
		simpleCollection     bool
		indexCollation, want bson.D
	}{
		{"simple collection", true, nil, simpleCollation},
		{"simple index on non-simple collection", false, nil, simpleCollation},
		{"non-simple index", false, englishCollation, englishCollation},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalog := idx.NewCatalog(&version.MongoVersion{Version: []int{8, 0}})
			catalog.SetCollation("db", "metrics", tt.simpleCollection)
			catalog.SetCollation("db", "stats", !tt.simpleCollection)
			index := &mongoidx.IndexDocument{
				Key: bson.D{{"value", 1}}, Options: bson.M{"name": "value_1"},
			}
			if tt.indexCollation != nil {
				index.Options["collation"] = tt.indexCollation
			}
			catalog.AddIndex("db", "metrics", index)
			catalog.RenameCollection("db", "metrics", "db", "stats")

			groups := catalog.BuildGroups("db", "stats")
			require.Len(t, groups, 1)
			require.Len(t, groups[0].Indexes, 1)
			require.Equal(t, tt.want, groups[0].Indexes[0].Options["collation"])
		})
	}
}

func TestCatalogRenameSameNamespace(t *testing.T) {
	catalog := idx.NewCatalog(&version.MongoVersion{Version: []int{8, 3}})
	catalog.AddOplogIndex("db", "system.buckets.metrics", &mongoidx.IndexDocument{
		Key: bson.D{{"meta.sensor", 1}}, Options: bson.M{"name": "sensor_1"},
	})
	want := catalog.BuildGroups("db", "metrics")
	catalog.RenameCollection("db", "metrics", "db", "system.buckets.metrics")
	require.Equal(t, want, catalog.BuildGroups("db", "metrics"))
}
