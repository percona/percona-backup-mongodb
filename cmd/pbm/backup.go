package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	plog "github.com/percona/percona-backup-mongodb/pbm/log"
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

func waitForBcpStatus(ctx context.Context, cn *pbm.PBM, bcpName string) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	var err error
	bmeta := new(pbm.BackupMeta)
	for {
		select {
		case <-tk.C:
			fmt.Print(".")
			bmeta, err = cn.GetBackupMeta(bcpName)
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
	fmt.Println("Backup snapshots:")
	for i := len(bcps) - 1; i >= 0; i-- {
		b := bcps[i]

		if b.Status != pbm.StatusDone {
			continue
		}

		fmt.Printf("  %s [complete: %s]\n", b.Name, fmtTS(int64(b.LastWriteTS.T)))
	}
}

func printPITR(cn *pbm.PBM, size int, full bool) {
	on, err := cn.IsPITR()
	if err != nil {
		log.Fatalf("Error: check if PITR is on: %v", err)
	}

	inf, err := cn.GetNodeInfo()
	if err != nil {
		log.Fatalf("Error: define cluster state: %v", err)
	}

	shards := []pbm.Shard{{RS: inf.SetName}}
	if inf.IsSharded() {
		s, err := cn.GetShards()
		if err != nil {
			log.Fatalf("Error: get shards: %v", err)
		}
		shards = append(shards, s...)
	}

	ts, err := cn.ClusterTime()
	if err != nil {
		log.Fatalf("Error: read cluster time: %v", err)
	}

	cfg, err := cn.GetConfig()
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return
		}

		log.Fatalf("Error: read config: %v", cfg)
	}

	epch, err := cn.GetEpoch()
	if err != nil {
		log.Printf("Error: get current epoch: %v", err)
	}
	now := time.Now().Unix()
	var pitrList, pitrErrors string
	var rstlines [][]pbm.Timeline
	for _, s := range shards {
		if on {
			err := pitrState(cn, s.RS, ts)
			if err == errPITRBackup && int64(epch.TS().T) <= time.Now().Add(-1*time.Minute).Unix() {
				pitrErrors += fmt.Sprintf("  %s: PITR backup didn't started\n", s.RS)
			} else if err != nil {
				log.Printf("Error: check PITR state for shard '%s': %v", s.RS, err)
			}
			lg, err := pitrLog(cn, s.RS, epch)
			if err != nil {
				log.Printf("Error: get log for shard '%s': %v", s.RS, err)
			}
			if lg != "" {
				pitrErrors += fmt.Sprintf("  %s: %s\n", s.RS, lg)
			}
		}

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

	if len(pitrErrors) > 0 {
		fmt.Printf("\n!Failed to run PITR backup. Agent logs:\n%s", pitrErrors)
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
