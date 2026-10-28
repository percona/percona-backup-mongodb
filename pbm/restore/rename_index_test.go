package restore

import (
	"testing"

	"github.com/mongodb/mongo-tools/common/db"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestReplayOplogRenameIndexes(t *testing.T) {
	for _, sourcePending := range []bool{false, true} {
		name := "source has no pending indexes"
		if sourcePending {
			name = "source has pending indexes"
		}
		t.Run(name, func(t *testing.T) {
			source := setupCollection(t, "phys_idx_rename", "metrics", mongo.IndexModel{
				Keys: bson.D{{"fieldX", 1}}, Options: options.Index().SetName("existing_idx"),
			})
			destination := source.Database().Collection("stats")
			_, err := destination.InsertOne(t.Context(), bson.D{{"old", true}})
			require.NoError(t, err)
			ops := []db.Oplog{createIndexesOp(t, destination, ts(110), "obsolete_idx", bson.D{{"old", 1}})}
			wantIndexes := []string{idIndexName, "existing_idx"}
			if sourcePending {
				ops = append(ops, createIndexesOp(t, source, ts(120), "new_idx", bson.D{{"new", 1}}))
				wantIndexes = append(wantIndexes, "new_idx")
			}
			ops = append(ops, cmdOp(t, source, ts(130), bson.D{
				{"renameCollection", source.Database().Name() + ".metrics"},
				{"to", source.Database().Name() + ".stats"},
				{"dropTarget", *collectionUUID(t, destination)},
			}))

			replayIndexOplog(t, ops...)

			assertIndexNames(t, destination, wantIndexes...)
			var data bson.M
			require.NoError(t, destination.FindOne(t.Context(), bson.D{}).Decode(&data))
			require.Equal(t, int32(1), data["fieldX"])
			require.NotContains(t, data, "old")
			names, err := source.Database().ListCollectionNames(t.Context(), bson.D{{"name", "metrics"}})
			require.NoError(t, err)
			require.Empty(t, names)
		})
	}
}
