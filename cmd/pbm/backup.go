package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	plog "github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/version"
)

func backup(cn *pbm.PBM, bcpName, compression string) (string, error) {
	locks, err := cn.GetLocks(&pbm.LockHeader{})
	if err != nil {
		log.Println("get locks", err)
	}

	ts, err := cn.ClusterTime()
	if err != nil {
		return "", errors.Wrap(err, "read cluster time")
	}

	// Stop if there is some live operation.
	// But if there is some stale lock leave it for agents to deal with.
	for _, l := range locks {
		if l.Heartbeat.T+pbm.StaleFrameSec >= ts.T && l.Type != pbm.CmdPITR {
			return "", errors.Errorf("another operation in progress, %s/%s [%s/%s]", l.Type, l.OPID, l.Replset, l.Node)
		}
	}

	cfg, err := cn.GetConfig()
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", errors.New("no store set. Set remote store with <pbm store set>")
		}
		return "", errors.Wrap(err, "get remote-store")
	}

	err = cn.SendCmd(pbm.Cmd{
		Cmd: pbm.CmdBackup,
		Backup: pbm.BackupCmd{
			Name:        bcpName,
			Compression: pbm.CompressionType(compression),
		},
	})
	if err != nil {
		return "", errors.Wrap(err, "send command")
	}

	ctx, cancel := context.WithTimeout(context.Background(), pbm.WaitBackupStart)
	defer cancel()
	err = waitForBcpStatus(ctx, cn, bcpName)
	if err != nil {
		return "", err
	}

	storeString := ""
	switch cfg.Storage.Type {
	case pbm.StorageS3:
		storeString = "s3://"
		if cfg.Storage.S3.EndpointURL != "" {
			storeString += cfg.Storage.S3.EndpointURL + "/"
		}
		storeString += cfg.Storage.S3.Bucket
		if cfg.Storage.S3.Prefix != "" {
			storeString += "/" + cfg.Storage.S3.Prefix
		}
	case pbm.StorageFilesystem:
		storeString = cfg.Storage.Filesystem.Path
	}
	return storeString, nil
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

func printBackupList(cn *pbm.PBM, size int64) {
	bcps, err := cn.BackupsList(size)
	if err != nil {
		log.Fatalln("Error: unable to get backups list:", err)
	}

	inf, err := cn.GetNodeInfo()
	if err != nil {
		log.Fatalln("Error: define cluster state", err)
	}

	shards, err := cn.ClusterMembers(inf)
	if err != nil {
		log.Fatalln("Error: get cluster members", err)
	}

	// pbm.PBM is always connected either to config server or to the sole (hence main) RS
	// which the `confsrv` param in `bcpMatchCluster` is all about
	bcpMatchCluster(bcps, shards, inf.SetName)

	fmt.Println("Backup snapshots:")
	for i := len(bcps) - 1; i >= 0; i-- {
		b := bcps[i]

		if b.Status != pbm.StatusDone {
			continue
		}
		var v string
		if !version.Compatible(version.DefaultInfo.Version, b.PBMVersion) {
			v = fmt.Sprintf(" !!! backup v%s is not compatible with PBM v%s", b.PBMVersion, version.DefaultInfo.Version)
		}

		fmt.Printf("  %s [complete: %s]%v\n", b.Name, fmtTS(int64(b.LastWriteTS.T)), v)
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
func bcpMatchCluster(bcps []pbm.BackupMeta, shards []pbm.Shard, confsrv string) {
	sh := make(map[string]struct{}, len(shards))
	for _, s := range shards {
		sh[s.RS] = struct{}{}
	}

	var nomatch []string
	for i := 0; i < len(bcps); i++ {
		bcp := &bcps[i]
		nomatch = nomatch[:0]
		hasconfsrv := false
		for _, rs := range bcp.Replsets {
			if _, ok := sh[rs.Name]; !ok {
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
}

func printPITR(cn *pbm.PBM, size int, full bool) {
	on, err := cn.IsPITR()
	if err != nil {
		log.Fatalf("Error: check if PITR is on: %v", err)
	}

	shards, err := cn.ClusterMembers(nil)
	if err != nil {
		log.Fatalf("Error: get cluster members: %v", err)
	}

	now := time.Now().Unix()
	var pitrList string
	var rstlines [][]pbm.Timeline
	for _, s := range shards {
		tlns, err := cn.PITRGetValidTimelines(s.RS, now, nil)
		if err != nil {
			log.Printf("Error: get PITR timelines for %s replset: %v", s.RS, err)
		}

		if len(tlns) == 0 {
			continue
		}

		if size > 0 && size < len(tlns) {
			tlns = tlns[len(tlns)-size:]
		}

		if full {
			rsout := fmt.Sprintf("  %s:", s.RS)
			for _, tln := range tlns {
				rsout += fmt.Sprintf(" %v,", tln)
			}
			pitrList += rsout[:len(rsout)-1] + "\n"
		} else {
			rstlines = append(rstlines, tlns)
		}
	}
	if len(pitrList) > 0 {
		fmt.Printf("PITR shards' timelines:\n%s", pitrList)
	}

	if on {
		fmt.Println("\nPITR <on>:")
	}
	if len(rstlines) > 0 && len(rstlines) == len(shards) {
		if !on {
			fmt.Println("\nPITR <off>:")
		}
		for _, tl := range pbm.MergeTimelines(rstlines...) {
			fmt.Println(" ", tl)
		}
	}
}

var errPITRBackup = errors.New("PITR backup failed")

func pitrState(cn *pbm.PBM, rs string, ts primitive.Timestamp) error {
	l, err := cn.GetLockData(&pbm.LockHeader{Replset: rs})
	if err == mongo.ErrNoDocuments {
		return errPITRBackup
	} else if err != nil {
		return errors.Wrap(err, "get lock")
	}

	switch l.Type {
	default:
		return errors.Errorf("undefined state, has lock for op `%v`", l.Type)
	case pbm.CmdPITR:
		if l.Heartbeat.T+pbm.StaleFrameSec < ts.T {
			return errPITRBackup
		}
	case pbm.CmdBackup: // running backup is ok
	}

	return nil
}

func pitrLog(cn *pbm.PBM, rs string, e pbm.Epoch) (string, error) {
	l, err := cn.LogGet(
		&plog.LogRequest{
			LogKeys: plog.LogKeys{
				RS:       rs,
				Severity: plog.Error,
				Event:    string(pbm.CmdPITR),
				Epoch:    e.TS(),
			},
		},
		1)
	if err != nil {
		return "", errors.Wrap(err, "get log records")
	}
	if len(l) == 0 {
		return "", nil
	}

	return l[0].String(), nil
}

func printBackupProgress(b pbm.BackupMeta, pbmClient *pbm.PBM) (string, error) {
	locks, err := pbmClient.GetLocks(&pbm.LockHeader{
		Type: pbm.CmdBackup,
		OPID: b.OPID,
	})

	if err != nil {
		return "", errors.Wrap(err, "get locks")
	}

	ts, err := pbmClient.ClusterTime()
	if err != nil {
		return "", errors.Wrap(err, "read cluster time")
	}

	stale := false
	staleMsg := "Stale: pbm-agents make no progress:"
	for _, l := range locks {
		if l.Heartbeat.T+pbm.StaleFrameSec < ts.T {
			stale = true
			staleMsg += fmt.Sprintf(" %s/%s [%s],", l.Replset, l.Node, time.Unix(int64(l.Heartbeat.T), 0).UTC().Format(time.RFC3339))
		}
	}

	if stale {
		return fmt.Sprintf("%s\t%s", b.Name, staleMsg[:len(staleMsg)-1]), nil
	}

	return fmt.Sprintf("%s\tIn progress [%s] (Launched at %s)", b.Name, b.Status, time.Unix(b.StartTS, 0).UTC().Format(time.RFC3339)), nil
}
