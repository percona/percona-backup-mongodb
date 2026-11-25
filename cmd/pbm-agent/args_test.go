package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMaskCreds(t *testing.T) {
	tests := []struct {
		name string
		uri  string
		want string
	}{
		{
			name: "tcp host",
			uri:  "mongodb://pbm:secret@localhost:27017/?authSource=admin",
			want: "mongodb://xxx:xxxxxx@localhost:27017/?authSource=admin",
		},
		{
			name: "percent-encoded password",
			uri:  "mongodb://pbm:p%40ss@rs101:27017,rs102:27017/?replicaSet=rs1",
			want: "mongodb://xxx:xxxxxx@rs101:27017,rs102:27017/?replicaSet=rs1",
		},
		{
			name: "unix socket",
			uri:  "mongodb://pbm:secret@%2Ftmp%2Fmongodb-27017.sock/?authSource=admin",
			want: "mongodb://xxx:xxxxxx@%2Ftmp%2Fmongodb-27017.sock/?authSource=admin",
		},
		{
			name: "no scheme",
			uri:  "pbm:secret@localhost:27017",
			want: "xxx:xxxxxx@localhost:27017",
		},
		{
			name: "user only",
			uri:  "mongodb://pbm@localhost:27017/?authMechanism=MONGODB-X509",
			want: "mongodb://xxx@localhost:27017/?authMechanism=MONGODB-X509",
		},
		{
			name: "no credentials",
			uri:  "mongodb://localhost:27017/",
			want: "mongodb://localhost:27017/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := maskCreds(tt.uri)
			assert.Equal(t, tt.want, got)
			// hidecreds overwrites the argument in place, so the length is kept.
			assert.Len(t, got, len(tt.uri))
		})
	}
}
