package cli

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
)

type backupOpts struct {
	name        string
	compression pbm.CompressionType
}

type backupOut struct {
	name    string
	storage string
}

func (b backupOut) String() string {
	return fmt.Sprintf("Backup '%s' to remote store '%s' has started", b.name, b.storage)
}

func runBackup(cn *pbm.PBM, b *backupOpts, outf outFormat) (fmt.Stringer, error) {
	locks, err := cn.GetLocks(&pbm.LockHeader{})
	if err != nil {
		log.Println("get locks", err)
	}

	ts, err := cn.ClusterTime()
	if err != nil {
		return nil, errors.Wrap(err, "read cluster time")
	}

	// Stop if there is some live operation.
	// But if there is some stale lock leave it for agents to deal with.
	for _, l := range locks {
		if l.Heartbeat.T+pbm.StaleFrameSec >= ts.T && l.Type != pbm.CmdPITR {
			return nil, errors.Errorf("another operation in progress, %s/%s [%s/%s]", l.Type, l.OPID, l.Replset, l.Node)
		}
	}

	cfg, err := cn.GetConfig()
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errors.New("no store set. Set remote store with <pbm store set>")
		}
		return nil, errors.Wrap(err, "get remote-store")
	}

	err = cn.SendCmd(pbm.Cmd{
		Cmd: pbm.CmdBackup,
		Backup: pbm.BackupCmd{
			Name:        b.name,
			Compression: pbm.CompressionType(b.compression),
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "send command")
	}

	ctx, cancel := context.WithTimeout(context.Background(), pbm.WaitBackupStart)
	defer cancel()
	err = waitForBcpStatus(ctx, cn, b.name, outf)
	if err != nil {
		return nil, err
	}

	return backupOut{b.name, cfg.Storage.Path()}, nil
}

func waitForBcpStatus(ctx context.Context, cn *pbm.PBM, bcpName string, outf outFormat) (err error) {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()

	var bmeta *pbm.BackupMeta
	for {
		select {
		case <-tk.C:
			if outf == outText {
				fmt.Print(".")
			}
			bmeta, err = cn.GetBackupMeta(bcpName)
			if errors.Is(err, pbm.ErrNotFound) {
				continue
			}
			if err != nil {
				return errors.Wrap(err, "get backup metadata")
			}
			switch bmeta.Status {
			case pbm.StatusRunning, pbm.StatusDumpDone, pbm.StatusDone:
				return nil
			case pbm.StatusError:
				rs := ""
				for _, s := range bmeta.Replsets {
					rs += fmt.Sprintf("\n- Backup on replicaset \"%s\" in state: %v", s.Name, s.Status)
					if s.Error != "" {
						rs += ": " + s.Error
					}
				}
				return errors.New(bmeta.Error + rs)
			}
		case <-ctx.Done():
			if bmeta == nil {
				return errors.New("no progress from leader, backup metadata not found")
			}
			rs := ""
			for _, s := range bmeta.Replsets {
				rs += fmt.Sprintf("- Backup on replicaset \"%s\" in state: %v\n", s.Name, s.Status)
				if s.Error != "" {
					rs += ": " + s.Error
				}
			}
			if rs == "" {
				rs = "<no replset has started backup>\n"
			}

			return errors.New("no confirmation that backup has successfully started. Replsets status:\n" + rs)
		}
	}
}

// bcpMatchCluster checks if given backups match shards in the cluster. Match means that
// each replset in backup have respective replset on the target cluster. It's ok if cluster
// has more shards than there are currently in backup. But in the case of sharded cluster
// backup has to have data for the current config server or for the sole RS in case of non-sharded rs.
//
// If some backup doesn't match cluster, the status of the backup meta in given `bcps` would be
// changed to pbm.StatusError with respective error text emitted. It doesn't change meta on
// storage nor in DB (backup is ok, it just doesn't cluster), it is just "in-flight" changes
// in given `bcps`.
func bcpsMatchCluster(bcps []pbm.BackupMeta, shards []pbm.Shard, confsrv string) {
	sh := make(map[string]struct{}, len(shards))
	for _, s := range shards {
		sh[s.RS] = struct{}{}
	}

	var nomatch []string
	for i := 0; i < len(bcps); i++ {
		bcpMatchCluster(&bcps[i], sh, confsrv, nomatch[:0])
	}
}

func bcpMatchCluster(bcp *pbm.BackupMeta, shards map[string]struct{}, confsrv string, nomatch []string) {
	hasconfsrv := false
	for _, rs := range bcp.Replsets {
		if _, ok := shards[rs.Name]; !ok {
			nomatch = append(nomatch, rs.Name)
		}
		if rs.Name == confsrv {
			hasconfsrv = true
		}
	}

	if len(nomatch) > 0 {
		bcp.Error = "Backup doesn't match current cluster topology - it has different replica set names. " +
			"Extra shards in the backup will cause this, for a simple example. " +
			"The extra/unknown replica set names found in the backup are: " + strings.Join(nomatch, ", ")
		bcp.Status = pbm.StatusError
	}

	if !hasconfsrv {
		if bcp.Error != "" {
			bcp.Error += ". "
		}
		bcp.Error += "Backup has no data for the config server or sole replicaset"
		bcp.Status = pbm.StatusError
	}
}
