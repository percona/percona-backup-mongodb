package task

import (
	"context"
	"encoding/json"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
)

var ErrAlreadyExists = errors.New("backup task already exists")

const leaseTTL = 10 * time.Second

// Composer puts cluster-wide tasks together and delegates the work to the
// agents. It is the central-coordination half of an operation:
// - the service owning the operation (e.g. backup) decides exact phases and actions,
// - composer select the agents by using scheduler,
// - composer makes the cluster aware of it by delegating tasks over etcd.
type Composer struct {
	ccDB *clientv3.Client
}

// NewComposer creates a task composer.
// ccDB holds the control state.
func NewComposer(ccDB *clientv3.Client) *Composer {
	return &Composer{ccDB: ccDB}
}

// Backup schedules a new backup:
// - picks the agents
// - records the task,
// - and drops an item into every participant's inbox.
// All of it shares a single lease.
// It returns as soon as the work is delegated; the backup itself runs on the
// agents.
func (c *Composer) Backup(ctx context.Context, name string) (*BackupTask, error) {
	agents, leader := selectAgents()

	now := time.Now()
	t := &BackupTask{
		Name:    name,
		Agents:  agents,
		Leader:  leader,
		StartTS: now.Unix(),
	}

	taskDoc, err := json.Marshal(t)
	if err != nil {
		return nil, errors.Wrap(err, "marshal backup task")
	}

	lease, err := c.ccDB.Grant(ctx, int64(leaseTTL.Seconds()))
	if err != nil {
		return nil, errors.Wrap(err, "grant backup task lease")
	}

	ops := make([]clientv3.Op, 0, len(agents)+1)
	ops = append(ops, clientv3.OpPut(backupKey(t.Name), string(taskDoc), clientv3.WithLease(lease.ID)))
	for _, agent := range agents {
		item, err := json.Marshal(InboxItem{
			Type:     TaskBackup,
			Task:     t.Name,
			IsLeader: agent == leader,
		})
		if err != nil {
			c.revoke(ctx, lease.ID)
			return nil, errors.Wrapf(err, "marshal item for %s", agent)
		}
		ops = append(ops,
			clientv3.OpPut(inboxKey(agent, t.Name), string(item), clientv3.WithLease(lease.ID)))
	}

	resp, err := c.ccDB.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(backupKey(t.Name)), "=", 0)).
		Then(ops...).
		Commit()
	if err != nil {
		c.revoke(ctx, lease.ID)
		return nil, errors.Wrap(err, "put backup task")
	}
	if !resp.Succeeded {
		c.revoke(ctx, lease.ID)
		return nil, ErrAlreadyExists
	}

	return t, nil
}

// revoke drops a lease whose keys were never written.
func (c *Composer) revoke(ctx context.Context, id clientv3.LeaseID) {
	if _, err := c.ccDB.Revoke(ctx, id); err != nil {
		log.Printf("task: revoke lease %d: %v", id, err)
	}
}
