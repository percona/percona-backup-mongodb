package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
)

type backupOpts struct {
	name             string
	typ              string
	compression      string
	compressionLevel []int
}

type backupOut struct {
	Name    string `json:"name"`
	Storage string `json:"storage"`
}

func (b backupOut) String() string {
	return fmt.Sprintf("Backup '%s' to remote store '%s' has started", b.Name, b.Storage)
}

func runBackup(cn *pbm.PBM, b *backupOpts, outf outFormat) (fmt.Stringer, error) {
	err := checkConcurrentOp(cn)
	if err != nil {
		// PITR slicing can be run along with the backup start - agents will resolve it.
		op, ok := err.(concurentOpErr)
		if !ok || op.op.Type != pbm.CmdPITR {
			return nil, err
		}
	}

	cfg, err := cn.GetConfig()
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errors.New("no store set. Set remote store with <pbm store set>")
		}
		return nil, errors.Wrap(err, "get remote-store")
	}

	var level *int
	if len(b.compressionLevel) > 0 {
		level = &b.compressionLevel[0]
	} else if cfg.Backup.CompressionLevel != nil {
		level = cfg.Backup.CompressionLevel
	}

	compression := pbm.CompressionType(b.compression)
	if compression == "" {
		if cfg.Backup.Compression != "" {
			compression = cfg.Backup.Compression
		} else {
			compression = pbm.CompressionTypeS2
		}
	}

	err = cn.SendCmd(pbm.Cmd{
		Cmd: pbm.CmdBackup,
		Backup: pbm.BackupCmd{
			Type:             pbm.BackupType(b.typ),
			Name:             b.name,
			Compression:      compression,
			CompressionLevel: level,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "send command")
	}

	if outf != outText {
		return backupOut{b.name, cfg.Storage.Path()}, nil
	}

	fmt.Printf("Starting backup '%s'", b.name)
	ctx, cancel := context.WithTimeout(context.Background(), pbm.WaitBackupStart)
	defer cancel()
	err = waitForBcpStatus(ctx, cn, b.name)
	if err != nil {
		return nil, err
	}

	fmt.Println()
	return backupOut{b.name, cfg.Storage.Path()}, nil
}

func waitForBcpStatus(ctx context.Context, cn *pbm.PBM, bcpName string) (err error) {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()

	var bmeta *pbm.BackupMeta
	for {
		select {
		case <-tk.C:
			fmt.Print(".")
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

// bcpsMatchCluster checks if given backups match shards in the cluster. Match means that
// each replset in backup has a respective replset on the target cluster. It's ok if cluster
// has more shards than there are currently in backup. But in the case of sharded cluster
// backup has to have data for the current config server or for the sole RS in case of non-sharded rs.
//
// If some backup doesn't match cluster, the status of the backup meta in given `bcps` would be
// changed to pbm.StatusError with respective error text emitted. It doesn't change meta on
// storage nor in DB (backup is ok, it just doesn't cluster), it is just "in-flight" changes
// in given `bcps`.
func bcpsMatchCluster(bcps []pbm.BackupMeta, shards []pbm.Shard, confsrv string, mapRS pbm.RSMapFunc) {
	sh := make(map[string]struct{}, len(shards))
	for _, s := range shards {
		sh[s.RS] = struct{}{}
	}

	var buf []string
	for i := 0; i < len(bcps); i++ {
		buf = buf[:0]
		bcpMatchCluster(&bcps[i], sh, confsrv, &buf, mapRS)
	}
}

func bcpMatchCluster(bcp *pbm.BackupMeta, shards map[string]struct{}, confsrv string, nomatch *[]string, mapRS pbm.RSMapFunc) {
	if bcp.Status != pbm.StatusDone {
		return
	}

	hasconfsrv := false
	for i := range bcp.Replsets {
		name := mapRS(bcp.Replsets[i].Name)
		if _, ok := shards[name]; !ok {
			*nomatch = append(*nomatch, name)
		}
		if name == confsrv {
			hasconfsrv = true
		}
	}

	if len(*nomatch) > 0 {
		bcp.Error = "Backup doesn't match current cluster topology - it has different replica set names. " +
			"Extra shards in the backup will cause this, for a simple example. " +
			"The extra/unknown replica set names found in the backup are: " + strings.Join(*nomatch, ", ")
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
