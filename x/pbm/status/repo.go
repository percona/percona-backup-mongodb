package status

import (
	"context"
	"encoding/json"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
)

const keyPrefix = "/pbm/agents/"

var ErrDocNotFound = errors.New("agent doc not found")

// Repo is the agent's state (mongod info, leader, api info, ...) repository.
type Repo struct {
	ccDB *clientv3.Client
}

func NewRepo(ccDB *clientv3.Client) *Repo {
	return &Repo{
		ccDB: ccDB,
	}
}

// Put stores the agent's doc and returns its revision.
func (r *Repo) Put(ctx context.Context, name string, doc agentDoc) (int64, error) {
	data, err := json.Marshal(doc)
	if err != nil {
		return 0, errors.Wrap(err, "marshal agent doc")
	}

	resp, err := r.ccDB.Put(ctx, key(name), string(data))
	if err != nil {
		return 0, errors.Wrap(err, "put agent doc")
	}
	// for a single put, the header revision is the key's ModRevision
	return resp.Header.Revision, nil
}

// Get returns the agent's doc as it was at the given etcd revision.
// It returns ErrDocNotFound if no doc was stored for the agent at that revision.
// It fails if the revision has been compacted.
func (r *Repo) Get(ctx context.Context, name string, rev int64) (agentDoc, error) {
	resp, err := r.ccDB.Get(ctx, key(name), clientv3.WithRev(rev))
	if err != nil {
		return agentDoc{}, errors.Wrapf(err, "get agent doc at rev %d", rev)
	}
	if len(resp.Kvs) == 0 {
		return agentDoc{}, ErrDocNotFound
	}

	var doc agentDoc
	if err := json.Unmarshal(resp.Kvs[0].Value, &doc); err != nil {
		return agentDoc{}, errors.Wrap(err, "unmarshal agent doc")
	}
	return doc, nil
}

func key(name string) string {
	return keyPrefix + name
}
