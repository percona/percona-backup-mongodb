package cli

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/yaml.v2"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/version"
)

type backupOpts struct {
	name             string
	typ              string
	compression      string
	compressionLevel []int
	ns               string
	wait             bool
}

type backupOut struct {
	Name    string `json:"name"`
	Storage string `json:"storage"`
}

func (b backupOut) String() string {
	return fmt.Sprintf("Backup '%s' to remote store '%s' has started", b.Name, b.Storage)
}

type descBcp struct {
	name string
}

func runBackup(cn *pbm.PBM, b *backupOpts, outf outFormat) (fmt.Stringer, error) {
	nss, err := parseCLINSOption(b.ns)
	if err != nil {
		return nil, errors.WithMessage(err, "parse --ns option")
	}
	if len(nss) > 1 {
		return nil, errors.New("parse --ns option: multiple namespaces are not supported")
	}

	if err := checkConcurrentOp(cn); err != nil {
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

	compression := compress.CompressionType(b.compression)
	if compression == "" {
		if cfg.Backup.Compression != "" {
			compression = cfg.Backup.Compression
		} else {
			compression = compress.CompressionTypeS2
		}
	}

	err = cn.SendCmd(pbm.Cmd{
		Cmd: pbm.CmdBackup,
		Backup: &pbm.BackupCmd{
			Type:             pbm.BackupType(b.typ),
			Name:             b.name,
			Namespaces:       nss,
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

	if b.wait {
		return outMsg{}, waitBackup(context.Background(), cn, b.name)
	}

	fmt.Println()
	return backupOut{b.name, cfg.Storage.Path()}, nil
}

func waitBackup(ctx context.Context, cn *pbm.PBM, name string) error {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	fmt.Printf("\nWaiting for '%s' backup...", name)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			bcp, err := cn.GetBackupMeta(name)
			if err != nil {
				return err
			}

			switch bcp.Status {
			case pbm.StatusDone:
				fmt.Println(" done")
				return nil
			case pbm.StatusCancelled:
				fmt.Println(" canceled")
				return nil
			case pbm.StatusError:
				fmt.Println(" failed")
				return bcp.Error()
			}
		}

		fmt.Print(".")
	}
}

func waitForBcpStatus(ctx context.Context, cn *pbm.PBM, bcpName string) (err error) {
	tk := time.NewTicker(time.Second)
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
			case pbm.StatusRunning, pbm.StatusDumpDone, pbm.StatusDone, pbm.StatusCancelled:
				return nil
			case pbm.StatusError:
				rs := ""
				for _, s := range bmeta.Replsets {
					rs += fmt.Sprintf("\n- Backup on replicaset \"%s\" in state: %v", s.Name, s.Status)
					if s.Error != "" {
						rs += ": " + s.Error
					}
				}
				return errors.New(bmeta.Error().Error() + rs)
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

type bcpDesc struct {
	Name         string              `json:"name" yaml:"name"`
	Type         pbm.BackupType      `json:"type" yaml:"type"`
	LastWriteTS  primitive.Timestamp `json:"last_write_ts" yaml:"last_write_ts"`
	Namespaces   []string            `json:"namespaces" yaml:"namespaces"`
	MongoVersion string              `json:"mongodb_version" yaml:"mongodb_version"`
	PBMVersion   string              `json:"pbm_version" yaml:"pbm_version"`
	Status       pbm.Status          `json:"status" yaml:"status"`
	Size         int64               `json:"size" yaml:"size"`
	Err          string              `json:"error" yaml:"error"`
	Replsets     []bcpReplDesc       `json:"replsets" yaml:"replsets"`
}

type bcpReplDesc struct {
	Name        string              `json:"name" yaml:"name"`
	Status      pbm.Status          `json:"status" yaml:"status"`
	IsConfigSvr bool                `json:"iscs" yaml:"iscs"`
	LastWriteTS primitive.Timestamp `json:"last_write_ts" yaml:"last_write_ts"`
	Error       string              `json:"error" yaml:"error"`
}

func (b *bcpDesc) String() string {
	data, err := yaml.Marshal(b)
	if err != nil {
		log.Fatal(err)
	}

	return string(data)
}

func describeBackup(cn *pbm.PBM, b *descBcp) (fmt.Stringer, error) {
	bcp, err := cn.GetBackupMeta(b.name)
	if err != nil {
		return nil, err
	}

	rv := &bcpDesc{
		Name:         bcp.Name,
		Type:         bcp.Type,
		Namespaces:   bcp.Namespaces,
		MongoVersion: bcp.MongoVersion,
		PBMVersion:   bcp.PBMVersion,
		LastWriteTS:  bcp.LastWriteTS,
		Status:       bcp.Status,
		Size:         bcp.Size,
		Err:          bcp.Err,
	}

	rv.Replsets = make([]bcpReplDesc, len(bcp.Replsets))
	for i, r := range bcp.Replsets {
		rv.Replsets[i] = bcpReplDesc{
			Name:        r.Name,
			IsConfigSvr: pbm.Deref(r.IsConfigSvr),
			Status:      r.Status,
			Error:       r.Error,
			LastWriteTS: r.LastWriteTS,
		}
	}

	return rv, err
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
func bcpsMatchCluster(bcps []pbm.BackupMeta, shards []pbm.Shard, confsrv string, rsMap map[string]string) {
	sh := make(map[string]bool, len(shards))
	for _, s := range shards {
		sh[s.RS] = s.RS == confsrv
	}

	mapRS, mapRevRS := pbm.MakeRSMapFunc(rsMap), pbm.MakeReverseRSMapFunc(rsMap)
	for i := 0; i < len(bcps); i++ {
		bcp := &bcps[i]
		if bcps[i].Type == pbm.PhysicalBackup && len(rsMap) != 0 {
			bcp.SetRuntimeError(errRSMappingWithPhysBackup{})
			continue
		}

		bcpMatchCluster(bcp, sh, mapRS, mapRevRS)
	}
}

func bcpMatchCluster(bcp *pbm.BackupMeta, shards map[string]bool, mapRS, mapRevRS pbm.RSMapFunc) {
	if bcp.Status != pbm.StatusDone {
		return
	}
	if !version.Compatible(version.DefaultInfo.Version, bcp.PBMVersion) {
		bcp.SetRuntimeError(errIncompatibleVersion{bcp.PBMVersion})
		return
	}

	var nomatch []string
	hasconfsrv := false
	for i := range bcp.Replsets {
		name := mapRS(bcp.Replsets[i].Name)

		isconfsrv, ok := shards[name]
		if !ok {
			nomatch = append(nomatch, name)
		} else if mapRevRS(name) != bcp.Replsets[i].Name {
			nomatch = append(nomatch, name)
		}

		if isconfsrv {
			hasconfsrv = true
		}
	}

	if len(nomatch) != 0 || !hasconfsrv {
		names := make([]string, len(nomatch))
		copy(names, nomatch)
		bcp.SetRuntimeError(errMissedReplsets{names: names, configsrv: !hasconfsrv})
	}
}

var errIncompatible = errors.New("incompatible")

type errRSMappingWithPhysBackup struct{}

func (errRSMappingWithPhysBackup) Error() string {
	return "unsupported with replset remapping"
}

func (errRSMappingWithPhysBackup) Unwrap() error {
	return errIncompatible
}

type errMissedReplsets struct {
	names     []string
	configsrv bool
}

func (e errMissedReplsets) Error() string {
	errString := ""
	if len(e.names) != 0 {
		errString = "Backup doesn't match current cluster topology - it has different replica set names. " +
			"Extra shards in the backup will cause this, for a simple example. " +
			"The extra/unknown replica set names found in the backup are: " + strings.Join(e.names, ", ")
	}

	if e.configsrv {
		if errString != "" {
			errString += ". "
		}
		errString += "Backup has no data for the config server or sole replicaset"
	}

	return errString
}

func (errMissedReplsets) Unwrap() error {
	return errIncompatible
}

type errIncompatibleVersion struct {
	bcpVer string
}

func (e errIncompatibleVersion) Unwrap() error {
	return errIncompatible
}

func (e errIncompatibleVersion) Error() string {
	return fmt.Sprintf("backup version (v%s) is not compatible with PBM v%s",
		e.bcpVer, version.DefaultInfo.Version)
}
