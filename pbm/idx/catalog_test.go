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

func TestCatalogIndexLifecycle(t *testing.T) {
	tests := []struct {
		name, collection, field string
	}{
		{"ordinary", "metrics", "m.sensor"},
		{"legacy time series", "system.buckets.metrics", "meta.sensor"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalog := idx.NewCatalog(&version.MongoVersion{Version: []int{8, 0, 0}})
			catalog.SetCollation("db", "metrics", true)
			catalog.AddIndexes("db", "metrics", []*mongoidx.IndexDocument{
				{Key: bson.D{{"m", 1}, {"ts", 1}}, Options: bson.M{"name": "m_1_ts_1"}},
				{Key: bson.D{{"m.sensor", 1}}, Options: bson.M{"name": "alias_idx"}},
			})

			err := catalog.DeleteIndexes("db", tt.collection, bson.D{
				{"dropIndexes", tt.collection}, {"index", "alias_idx"},
			})
			require.NoError(t, err)
			require.Nil(t, catalog.GetIndex("db", "metrics", "alias_idx"))
			require.Equal(t, []*mongoidx.IndexDocument{
				{Key: bson.D{{"m", 1}, {"ts", 1}}, Options: bson.M{"name": "m_1_ts_1"}},
			}, catalog.GetIndexes("db", "metrics"))

			catalog.AddOplogIndex("db", tt.collection, &mongoidx.IndexDocument{
				Key: bson.D{{tt.field, 1}}, Options: bson.M{"name": "alias_idx", "sparse": true},
			})
			wantRecreated := &mongoidx.IndexDocument{
				Key: bson.D{{tt.field, 1}}, Options: bson.M{"name": "alias_idx", "sparse": true},
			}
			require.Equal(t, wantRecreated, catalog.GetIndex("db", tt.collection, "alias_idx"))
			catalog.AddOplogIndex("db", tt.collection, &mongoidx.IndexDocument{
				Key: bson.D{{tt.field, 1}}, Options: bson.M{"name": "new_idx"},
			})
			require.ElementsMatch(t, []*mongoidx.IndexDocument{
				{Key: bson.D{{"m", 1}, {"ts", 1}}, Options: bson.M{"name": "m_1_ts_1"}},
				{Key: bson.D{{tt.field, 1}}, Options: bson.M{"name": "new_idx"}},
				wantRecreated,
			}, catalog.GetIndexes("db", tt.collection))
			require.Equal(t, []options.Namespace{{DB: "db", Collection: "metrics"}}, catalog.Namespaces())
		})
	}
}

func TestCatalogBuildGroups(t *testing.T) {
	tests := []struct {
		name             string
		targetVersion    []int
		bucketCollection string
		rawData          bool
	}{
		{"8.0", []int{8, 0, 0}, "system.buckets.metrics", false},
		{"8.3", []int{8, 3}, "metrics", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalog := idx.NewCatalog(&version.MongoVersion{Version: tt.targetVersion})
			catalog.SetCollation("db", "metrics", true)
			logical := &mongoidx.IndexDocument{
				Key:                     bson.D{{"m.sensor", 1}, {"ts", 1}},
				PartialFilterExpression: &bson.D{{"m.region", "west"}},
				Options: bson.M{
					"name": "logical", "ns": "source.metrics", "v": int32(2),
					"collation": bson.D{{"locale", "en"}},
				},
			}
			bucket := &mongoidx.IndexDocument{
				Key:                     bson.D{{"meta.sensor", 1}, {"control.min.ts", 1}, {"control.max.ts", 1}},
				PartialFilterExpression: &bson.D{{"meta.region", "west"}},
				Options:                 bson.M{"name": "bucket", "ns": "source.system.buckets.metrics", "v": int32(2)},
			}
			catalog.AddIndexes("db", "metrics", []*mongoidx.IndexDocument{logical})
			catalog.AddOplogIndexes("db", "system.buckets.metrics", []*mongoidx.IndexDocument{bucket})

			groups := catalog.BuildGroups("db", "metrics")
			require.Equal(t, []idx.BuildGroup{
				{
					Collection: "metrics",
					Indexes: []*mongoidx.IndexDocument{{
						Key:                     bson.D{{"m.sensor", 1}, {"ts", 1}},
						PartialFilterExpression: &bson.D{{"m.region", "west"}},
						Options: bson.M{
							"name": "logical", "ns": "db.metrics", "collation": bson.D{{"locale", "en"}},
						},
					}},
				},
				{
					Collection: tt.bucketCollection,
					RawData:    tt.rawData,
					Indexes: []*mongoidx.IndexDocument{{
						Key:                     bson.D{{"meta.sensor", 1}, {"control.min.ts", 1}, {"control.max.ts", 1}},
						PartialFilterExpression: &bson.D{{"meta.region", "west"}},
						Options:                 bson.M{"name": "bucket", "ns": "db." + tt.bucketCollection},
					}},
				},
			}, groups)

			require.Equal(t, "source.metrics", logical.Options["ns"])
			require.Equal(t, "source.system.buckets.metrics", bucket.Options["ns"])
			for i, original := range []*mongoidx.IndexDocument{logical, bucket} {
				require.Equal(t, int32(2), original.Options["v"])
				groups[i].Indexes[0].Options["hidden"] = true
				require.NotContains(t, original.Options, "hidden")
			}
		})
	}
}

func TestCatalogOrdinaryBuildGroup(t *testing.T) {
	catalog := idx.NewCatalog(&version.MongoVersion{Version: []int{8, 3, 0}})
	catalog.SetCollation("db", "ordinary", true)
	catalog.AddIndex("db", "ordinary", &mongoidx.IndexDocument{
		Key: bson.D{{"value", 1}}, Options: bson.M{"name": "metadata_idx"},
	})
	oplogIndex := &mongoidx.IndexDocument{
		Key: bson.D{{"meta.sensor", 1}}, Options: bson.M{"name": "oplog_idx"},
	}
	// Ordinary oplog input must clear a previous bucket routing marker.
	catalog.AddOplogIndex("db", "system.buckets.ordinary", oplogIndex)
	catalog.AddOplogIndex("db", "ordinary", oplogIndex)

	groups := catalog.BuildGroups("db", "ordinary")
	require.Len(t, groups, 1)
	require.Equal(t, "ordinary", groups[0].Collection)
	require.False(t, groups[0].RawData)
	require.ElementsMatch(t, []*mongoidx.IndexDocument{
		{Key: bson.D{{"value", 1}}, Options: bson.M{"name": "metadata_idx", "ns": "db.ordinary"}},
		{Key: bson.D{{"meta.sensor", 1}}, Options: bson.M{"name": "oplog_idx", "ns": "db.ordinary"}},
	}, groups[0].Indexes)
}

func TestCatalogBuildGroupsIDFiltering(t *testing.T) {
	logicalID := &mongoidx.IndexDocument{
		Key: bson.D{{"_id", 1}}, Options: bson.M{"name": "logical_id"},
	}
	bucketID := &mongoidx.IndexDocument{
		Key: bson.D{{"_id", 1}}, Options: bson.M{"name": "bucket_id"},
	}
	compoundID := &mongoidx.IndexDocument{
		Key: bson.D{{"_id", 1}, {"value", 1}}, Options: bson.M{"name": "_id_"},
	}
	tests := []struct {
		name    string
		logical []*mongoidx.IndexDocument
		bucket  []*mongoidx.IndexDocument
		want    []string
	}{
		{name: "empty catalog"},
		{name: "logical id only", logical: []*mongoidx.IndexDocument{logicalID}},
		{name: "bucket id only", bucket: []*mongoidx.IndexDocument{bucketID}},
		{
			name: "compound id is kept", logical: []*mongoidx.IndexDocument{logicalID, compoundID},
			want: []string{"_id_"},
		},
		{
			name:    "id excluded from both groups",
			logical: []*mongoidx.IndexDocument{logicalID},
			bucket:  []*mongoidx.IndexDocument{bucketID},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalog := idx.NewCatalog(&version.MongoVersion{Version: []int{8, 3, 0}})
			catalog.AddIndexes("db", "metrics", tt.logical)
			catalog.AddOplogIndexes("db", "system.buckets.metrics", tt.bucket)
			var names []string
			for _, group := range catalog.BuildGroups("db", "metrics") {
				require.NotEmpty(t, group.Indexes)
				for _, index := range group.Indexes {
					names = append(names, index.Options["name"].(string))
				}
			}
			require.ElementsMatch(t, tt.want, names)
		})
	}
}

func TestCatalogIndexSources(t *testing.T) {
	catalog := idx.NewCatalog(&version.MongoVersion{Version: []int{8, 3, 0}})
	catalog.SetCollation("db", "metrics", true)
	catalog.AddIndex("db", "metrics", &mongoidx.IndexDocument{
		Key: bson.D{{"m.sensor", 1}}, Options: bson.M{"name": "alias_idx"},
	})
	catalog.AddOplogIndex("db", "system.buckets.metrics", &mongoidx.IndexDocument{
		Key: bson.D{{"meta.sensor", 1}}, Options: bson.M{"name": "alias_idx", "sparse": true},
	})

	err := catalog.CollMod("db", "metrics", bson.D{{"name", "alias_idx"}, {"hidden", true}})
	require.NoError(t, err)
	groups := catalog.BuildGroups("db", "system.buckets.metrics")
	require.Len(t, groups, 1)
	require.True(t, groups[0].RawData, "a modification through the logical name must not reclassify the specification")
	require.Len(t, groups[0].Indexes, 1)
	require.Equal(t, &mongoidx.IndexDocument{
		Key:     bson.D{{"meta.sensor", 1}},
		Options: bson.M{"name": "alias_idx", "ns": "db.metrics", "sparse": true, "hidden": true},
	}, groups[0].Indexes[0])

	// A logical specification still uses direct creation when supplied through the bucket alias.
	catalog.AddIndex("db", "system.buckets.metrics", &mongoidx.IndexDocument{
		Key: bson.D{{"m.sensor", 1}}, Options: bson.M{"name": "alias_idx"},
	})
	err = catalog.CollMod("db", "system.buckets.metrics", bson.D{{"name", "alias_idx"}, {"hidden", true}})
	require.NoError(t, err)
	groups = catalog.BuildGroups("db", "metrics")
	require.Len(t, groups, 1)
	require.False(t, groups[0].RawData)
	require.Equal(t, "metrics", groups[0].Collection)
	require.Equal(t, bson.D{{"m.sensor", 1}}, groups[0].Indexes[0].Key)
	require.Equal(t, true, groups[0].Indexes[0].Options["hidden"])
}

func TestCatalogDeleteIndexRepresentations(t *testing.T) {
	tests := []struct {
		name     string
		selector any
		wantAll  bool
	}{
		{"name", "sensor_1", false},
		{"bucket key pattern", bson.D{{"meta.sensor", 1}}, false},
		{"all", "*", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			catalog := idx.NewCatalog(&version.MongoVersion{Version: []int{8, 3, 0}})
			catalog.AddIndex("db", "metrics", &mongoidx.IndexDocument{
				Key: bson.D{{"ts", 1}}, Options: bson.M{"name": "ts_1"},
			})
			catalog.AddOplogIndex("db", "system.buckets.metrics", &mongoidx.IndexDocument{
				Key: bson.D{{"meta.sensor", 1}}, Options: bson.M{"name": "sensor_1"},
			})

			err := catalog.DeleteIndexes("db", "system.buckets.metrics", bson.D{
				{"dropIndexes", "system.buckets.metrics"}, {"index", test.selector},
			})
			require.NoError(t, err)
			groups := catalog.BuildGroups("db", "metrics")
			if test.wantAll {
				require.Empty(t, groups)
			} else {
				require.Len(t, groups, 1)
				require.False(t, groups[0].RawData)
				require.Len(t, groups[0].Indexes, 1)
				require.Equal(t, "ts_1", groups[0].Indexes[0].Options["name"])
			}
		})
	}
}

func TestCatalogDropAliases(t *testing.T) {
	catalog := idx.NewCatalog(&version.MongoVersion{Version: []int{8, 3, 0}})
	for _, database := range []string{"db", "other"} {
		catalog.AddOplogIndex(database, "system.buckets.metrics", &mongoidx.IndexDocument{
			Key: bson.D{{"meta.sensor", 1}}, Options: bson.M{"name": "sensor_1"},
		})
	}
	catalog.DropCollection("db", "metrics")
	require.Nil(t, catalog.GetIndex("db", "system.buckets.metrics", "sensor_1"))
	require.Empty(t, catalog.BuildGroups("db", "metrics"))
	require.NotNil(t, catalog.GetIndex("other", "metrics", "sensor_1"))
	catalog.DropDatabase("other")
	require.Empty(t, catalog.Namespaces())
	require.Empty(t, catalog.BuildGroups("other", "metrics"))
}
