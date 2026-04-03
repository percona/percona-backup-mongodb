package connect

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
