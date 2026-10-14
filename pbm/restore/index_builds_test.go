package restore

import (
	"testing"

	mongoidx "github.com/mongodb/mongo-tools/common/idx"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	pbmidx "github.com/percona/percona-backup-mongodb/pbm/idx"
)

func TestCreateIndexesCommand(t *testing.T) {
	indexes := []*mongoidx.IndexDocument{{Key: bson.D{{"value", 1}}, Options: bson.M{"name": "value_1"}}}
	tests := []struct {
		name         string
		rawData      bool
		commitQuorum any
		wantFields   bson.D
	}{
		{name: "standalone"},
		{name: "replica set", commitQuorum: int32(3), wantFields: bson.D{{"commitQuorum", int32(3)}}},
		{name: "raw standalone", rawData: true, wantFields: bson.D{{"rawData", true}}},
		{
			name: "raw replica set", rawData: true, commitQuorum: "votingMembers",
			wantFields: bson.D{{"rawData", true}, {"commitQuorum", "votingMembers"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			group := pbmidx.BuildGroup{Collection: "metrics", RawData: tt.rawData, Indexes: indexes}
			want := bson.D{
				{"createIndexes", "metrics"},
				{"indexes", indexes},
				{"ignoreUnknownIndexOptions", true},
			}
			want = append(want, tt.wantFields...)
			require.Equal(t, want, createIndexesCommand(group, tt.commitQuorum))
		})
	}
}
