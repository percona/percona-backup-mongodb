package backup

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
)

const keyPrefix = "/pbm/backups/"

var (
	ErrNotFound      = errors.New("backup not found")
	ErrAlreadyExists = errors.New("backup already exists")
	ErrNoName        = errors.New("backup name is empty")
	ErrConflict      = errors.New("backup meta conflict")
	ErrNoRSName      = errors.New("replset name is empty")
	ErrRSNotFound    = errors.New("replset not found in backup")
)

const (
	// maxModifyAttempts bounds the read-modify-write loop
	maxModifyAttempts = 10
	// modifyBackoff is the delay before the first retry of a lost race
	modifyBackoff = 5 * time.Millisecond
	// modifyMaxBackoff caps the delay between attempts
	modifyMaxBackoff = 200 * time.Millisecond
)

// Repo is the backup repository.
// It manages backup metadata documents persisted in etcd as part of PBM's
// control-collection state. Each document is stored under
// "/pbm/backups/{name}" with a JSON-encoded BackupMeta value.
type Repo struct {
	ccDB *clientv3.Client
}

// New creates a backup repository. ccDB persists the metadata.
func New(ccDB *clientv3.Client) *Repo {
	return &Repo{
		ccDB: ccDB,
	}
}

// Get returns the backup metadata under the given name.
func (r *Repo) Get(ctx context.Context, name string) (*BackupMeta, error) {
	if name == "" {
		return nil, ErrNoName
	}

	resp, err := r.ccDB.Get(ctx, key(name))
	if err != nil {
		return nil, errors.Wrap(err, "get backup meta")
	}
	if len(resp.Kvs) == 0 {
		return nil, ErrNotFound
	}

	meta := &BackupMeta{}
	if err := json.Unmarshal(resp.Kvs[0].Value, meta); err != nil {
		return nil, errors.Wrap(err, "unmarshal backup meta")
	}

	return meta, nil
}

// GetAll returns every stored backup metadata, ordered by name ascending.
// As backup names are timestamps, this yields chronological order.
// It returns an empty slice when no backups exist.
func (r *Repo) GetAll(ctx context.Context) ([]*BackupMeta, error) {
	resp, err := r.ccDB.Get(ctx, keyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Wrap(err, "get backups")
	}

	out := make([]*BackupMeta, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		meta := &BackupMeta{}
		if err := json.Unmarshal(kv.Value, meta); err != nil {
			return nil, errors.Wrapf(err, "unmarshal backup meta %s", string(kv.Key))
		}
		out = append(out, meta)
	}

	return out, nil
}

// Insert stores a new backup metadata document.
// It returns ErrAlreadyExists if a backup with that name is already present.
func (r *Repo) Insert(ctx context.Context, meta *BackupMeta) error {
	if meta.Name == "" {
		return ErrNoName
	}

	data, err := json.Marshal(meta)
	if err != nil {
		return errors.Wrap(err, "marshal backup")
	}

	k := key(meta.Name)
	resp, err := r.ccDB.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(k), "=", 0)).
		Then(clientv3.OpPut(k, string(data))).
		Commit()
	if err != nil {
		return errors.Wrap(err, "insert backup meta")
	}
	if !resp.Succeeded {
		return ErrAlreadyExists
	}

	return nil
}

// UpdateRSMeta adds or replaces the section of rs.Name in the backup metadata.
// It returns ErrNoRSName when rs is nil or carries no name, as such a section
// could never be addressed again.
func (r *Repo) UpdateRSMeta(ctx context.Context, name string, rs *BackupReplset) error {
	if rs == nil || rs.Name == "" {
		return ErrNoRSName
	}

	_, err := r.modify(ctx, name, func(meta *BackupMeta) error {
		for i := range meta.Replsets {
			if meta.Replsets[i].Name == rs.Name {
				meta.Replsets[i] = *rs
				return nil
			}
		}
		meta.Replsets = append(meta.Replsets, *rs)

		return nil
	})

	return err
}

// SetFirstLastWrite records the cluster-wide first and last write timestamps.
// It returns ErrNotFound if no such backup exists.
func (r *Repo) SetFirstLastWrite(
	ctx context.Context,
	name string,
	first, last bson.Timestamp,
) error {
	_, err := r.modify(ctx, name, func(meta *BackupMeta) error {
		meta.FirstWriteTS = first
		meta.LastWriteTS = last
		return nil
	})

	return err
}

// SetSize records the cluster-wide backup size on the storage, compressed
// and uncompressed.
// It returns ErrNotFound if no such backup exists.
func (r *Repo) SetSize(ctx context.Context, name string, size, sizeUncompressed int64) error {
	_, err := r.modify(ctx, name, func(meta *BackupMeta) error {
		meta.Size = size
		meta.SizeUncompressed = sizeUncompressed
		return nil
	})

	return err
}

// SetRSSize records on the rsName section the size of that replset's backup
// on the storage, compressed and uncompressed.
// It returns ErrNotFound if no such backup exists and ErrRSNotFound if RS doesn't exist.
func (r *Repo) SetRSSize(
	ctx context.Context,
	name, rsName string,
	size, sizeUncompressed int64,
) error {
	if rsName == "" {
		return ErrNoRSName
	}

	_, err := r.modify(ctx, name, func(meta *BackupMeta) error {
		for i := range meta.Replsets {
			if meta.Replsets[i].Name == rsName {
				meta.Replsets[i].Size = size
				meta.Replsets[i].SizeUncompressed = sizeUncompressed
				return nil
			}
		}

		return ErrRSNotFound
	})

	return err
}

// SetFinishTime records when the backup ended and set done status.
// It returns ErrNotFound if no such backup exists.
func (r *Repo) SetFinishTime(ctx context.Context, name string, ts int64) error {
	_, err := r.modify(ctx, name, func(meta *BackupMeta) error {
		meta.FinishTime = ts
		meta.Status = StatusDone
		return nil
	})

	return err
}

// SetError records the error the backup failed with error status.
// It returns ErrNotFound if no such backup exists.
func (r *Repo) SetError(ctx context.Context, name string, cause error) error {
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}

	_, err := r.modify(ctx, name, func(meta *BackupMeta) error {
		meta.Error = msg
		meta.FinishTime = time.Now().UTC().Unix()
		meta.Status = StatusError
		return nil
	})

	return err
}

// SetRSError sets on the rsName section the error that replset failed with,
// and the error status.
// It returns ErrNotFound if no such backup exists and ErrRSNotFound if RS doesn't exist.
func (r *Repo) SetRSError(ctx context.Context, name, rsName string, cause error) error {
	if rsName == "" {
		return ErrNoRSName
	}

	msg := ""
	if cause != nil {
		msg = cause.Error()
	}

	_, err := r.modify(ctx, name, func(meta *BackupMeta) error {
		for i := range meta.Replsets {
			if meta.Replsets[i].Name == rsName {
				meta.Replsets[i].Error = msg
				meta.Replsets[i].Status = StatusError
				return nil
			}
		}

		return ErrRSNotFound
	})

	return err
}

// SetRSDone moves the rsName section to the done status.
// It returns ErrNotFound if no such backup exists and ErrRSNotFound if RS doesn't exist.
func (r *Repo) SetRSDone(ctx context.Context, name, rsName string) error {
	if rsName == "" {
		return ErrNoRSName
	}

	_, err := r.modify(ctx, name, func(meta *BackupMeta) error {
		for i := range meta.Replsets {
			if meta.Replsets[i].Name == rsName {
				meta.Replsets[i].Status = StatusDone
				return nil
			}
		}

		return ErrRSNotFound
	})

	return err
}

// Delete removes the backup metadata document.
// It returns ErrNotFound if no such backup exists.
func (r *Repo) Delete(ctx context.Context, name string) error {
	if name == "" {
		return ErrNoName
	}

	resp, err := r.ccDB.Delete(ctx, key(name))
	if err != nil {
		return errors.Wrap(err, "delete backup")
	}
	if resp.Deleted == 0 {
		return ErrNotFound
	}

	return nil
}

// DeleteAll removes every stored backup metadata document and returns the
// number of backups deleted.
func (r *Repo) DeleteAll(ctx context.Context) (int64, error) {
	resp, err := r.ccDB.Delete(ctx, keyPrefix, clientv3.WithPrefix())
	if err != nil {
		return 0, errors.Wrap(err, "delete backups")
	}

	return resp.Deleted, nil
}

// modify applies fn to the backup metadata stored under name and writes the
// result back.
// Retrying the read-modify-write when a concurrent writer wins the race.
func (r *Repo) modify(
	ctx context.Context,
	name string,
	fn func(*BackupMeta) error,
) (*BackupMeta, error) {
	if name == "" {
		return nil, ErrNoName
	}

	k := key(name)
	for attempt := range maxModifyAttempts {
		if attempt > 0 {
			backoff(attempt)
		}

		resp, err := r.ccDB.Get(ctx, k)
		if err != nil {
			return nil, errors.Wrap(err, "get backup meta")
		}
		if len(resp.Kvs) == 0 {
			return nil, ErrNotFound
		}

		meta := &BackupMeta{}
		if err := json.Unmarshal(resp.Kvs[0].Value, meta); err != nil {
			return nil, errors.Wrap(err, "unmarshal backup meta")
		}

		if err := fn(meta); err != nil {
			return nil, err
		}

		data, err := json.Marshal(meta)
		if err != nil {
			return nil, errors.Wrap(err, "marshal backup")
		}

		// document must still be at the revision it was read at
		txnResp, err := r.ccDB.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(k), "=", resp.Kvs[0].ModRevision)).
			Then(clientv3.OpPut(k, string(data))).
			Commit()
		if err != nil {
			return nil, errors.Wrap(err, "put backup meta")
		}
		if txnResp.Succeeded {
			return meta, nil
		}
	}

	return nil, ErrConflict
}

// backoff waits before retrying a lost race between rendomly 5-200ms.
func backoff(attempt int) {
	shift := min(attempt-1, 16)
	d := min(modifyBackoff<<shift, modifyMaxBackoff)

	time.Sleep(rand.N(d))
}

// key resolves the backup name to its etcd key.
func key(name string) string {
	return keyPrefix + name
}
