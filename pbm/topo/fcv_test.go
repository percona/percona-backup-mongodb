package topo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterFCVConnURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		baseURI     string
		replsetHost string
		want        string
		wantErr     bool
	}{
		{
			name:        "replace host and remove replica set",
			baseURI:     "mongodb://user:pass@mongos:27017/admin?authSource=admin&replicaSet=wrongRS&tls=true",
			replsetHost: "cfgRS/cfg1:27017,cfg2:27017",
			want:        "mongodb://user:pass@cfg1:27017,cfg2:27017/admin?authSource=admin&tls=true",
		},
		{
			name:        "empty base uri",
			replsetHost: "cfgRS/cfg1:27017",
			wantErr:     true,
		},
		{
			name:        "invalid config server uri",
			baseURI:     "mongodb://mongos:27017",
			replsetHost: "cfgRS",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := clusterFCVConnURI(tt.baseURI, tt.replsetHost)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
