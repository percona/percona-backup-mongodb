package restore

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
)

func TestFetchAvailability(t *testing.T) {
	isConfigSrv := true
	bcp := &backup.BackupMeta{
		Replsets: []backup.BackupReplset{
			{Name: "cfg", IsConfigSvr: &isConfigSrv, DumpName: "backup/cfg/metadata.json"},
		},
	}
	tests := []struct {
		name       string
		configSize int64
		userSize   int64
		want       bool
	}{
		{
			name:       "empty user collections do not hide nonempty routing collections",
			configSize: 100,
			userSize:   0,
			want:       true,
		},
		{
			name:       "nonempty user collections do not mark empty routing collections available",
			configSize: 0,
			userSize:   100,
			want:       false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// User entries follow config entries to expose collection-name-only
			// matching that would overwrite the routing availability flags.
			var namespaces []bson.M
			for _, db := range []string{defs.ConfigDB, "test"} {
				size := tt.configSize
				if db != defs.ConfigDB {
					size = tt.userSize
				}
				for _, coll := range []string{databasesNS, collectionsNS, chunksNS} {
					namespaces = append(namespaces, bson.M{"db": db, "collection": coll, "size": size})
				}
			}
			data, err := bson.MarshalExtJSON(bson.M{"namespaces": namespaces}, true, false)
			require.NoError(t, err)

			got, err := fetchAvailability(bcp, newTestStorage(bytes.NewBuffer(data)))
			require.NoError(t, err)
			require.Equal(t, map[string]bool{
				databasesNS:   tt.want,
				collectionsNS: tt.want,
				chunksNS:      tt.want,
			}, got)
		})
	}
}
