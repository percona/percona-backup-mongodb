package connect

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnsureMongoScheme(t *testing.T) {
	tests := []struct {
		name string
		uri  string
		want string
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ensureMongoScheme(tt.uri)
			assert.Equal(t, tt.want, got)
		})
	}
}
