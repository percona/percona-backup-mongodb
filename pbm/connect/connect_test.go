package connect

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A unix socket host is percent-encoded in the mongo-uri, as the connection
// string spec requires, so that URI is not a valid URL. Moving a connection to
// other hosts, like a shard's to its config server, must not parse it as one.
func TestClientOptionsWithOtherHosts(t *testing.T) {
	tests := []struct {
		name string
		uri  string
	}{
		{
			name: "tcp host",
			uri:  "mongodb://user:p%40ss@rs101:27017/?authSource=admin&replicaSet=rs1",
		},
		{
			name: "unix socket",
			uri:  "mongodb://user:p%40ss@%2Ftmp%2Fmongodb-27017.sock/?authSource=admin&replicaSet=rs1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := clientOptions(tt.uri, NoRS(), Hosts("cfg01:27019", "cfg02:27019"))
			require.NoError(t, err)

			assert.Equal(t, []string{"cfg01:27019", "cfg02:27019"}, opts.Hosts)
			require.NotNil(t, opts.ReplicaSet)
			assert.Empty(t, *opts.ReplicaSet)
			require.NotNil(t, opts.Auth)
			assert.Equal(t, "user", opts.Auth.Username)
			assert.Equal(t, "p@ss", opts.Auth.Password)
			assert.Equal(t, "admin", opts.Auth.AuthSource)
		})
	}
}

func TestEnsureMongoScheme(t *testing.T) {
	tests := []struct {
		name    string
		uri     string
		want    string
		wantErr string
	}{
		{
			name: "with scheme",
			uri:  "mongodb://user:pass@host:27017",
			want: "mongodb://user:pass@host:27017",
		},
		{
			name: "host only",
			uri:  "host:27017",
			want: "mongodb://host:27017",
		},
		{
			name: "with credentials",
			uri:  "user:pass@host:27017",
			want: "mongodb://user:pass@host:27017",
		},
		{
			name:    "srv scheme",
			uri:     "mongodb+srv://user:pass@cluster.example.com",
			wantErr: "mongodb+srv:// URI scheme is not supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ensureMongoScheme(tt.uri)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
