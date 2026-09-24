package connect

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
)

// ConnectDirect establishes a direct connection to the agent's local mongod.
//
// When uri is empty the agent has no attached MongoDB (e.g. a ctrl agent that
// only runs etcd), it returns a nil client and no error, and callers must treat
// a nil client as "no local mongod".
func ConnectDirect(ctx context.Context, uri string) (*mongo.Client, error) {
	if uri == "" {
		return nil, nil
	}

	client, err := MongoConnect(
		ctx,
		uri,
		AppName("pbmx-agent"),
		Direct(true),
		NoRS(),
	)
	if err != nil {
		return nil, errors.Wrap(err, "connect to local mongod")
	}

	return client, nil
}
