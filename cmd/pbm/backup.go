package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
)

func backup(cn *pbm.PBM, bcpName, compression string) (string, error) {
	locks, err := cn.GetLocks(&pbm.LockHeader{}, pbm.LockCollection)
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
			return "", errors.Errorf("another operation in progress, %s/%s", l.Type, l.BackupName)
		}
	}

	cfg, err := cn.GetConfig()
	if err != nil {
		if err == mongo.ErrNoDocuments {
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
	fmt.Println("Backup history:")
	for i := len(bcps) - 1; i >= 0; i-- {
		b := bcps[i]
		var bcp string
		switch b.Status {
		case pbm.StatusDone:
			bcp = b.Name
		case pbm.StatusCancelled:
			bcp = fmt.Sprintf("%s\tCancelled at %s", b.Name, time.Unix(b.LastTransitionTS, 0).UTC().Format(time.RFC3339))
		case pbm.StatusError:
			bcp = fmt.Sprintf("%s\tFailed with \"%s\"", b.Name, b.Error)
		default:
			bcp, err = printBackupProgress(b, cn)
			if err != nil {
				log.Fatalf("Error: list backup %s: %v\n", b.Name, err)
			}
		}

		fmt.Println(" ", bcp)
	}
}

func printPITR(cn *pbm.PBM) {
	on, err := cn.IsPITR()
	if err != nil {
		log.Fatalf("Error: check if PITR is on: %v", err)
	}

	im, err := cn.GetIsMaster()
	if err != nil {
		log.Fatalf("Error: define cluster state: %v", err)
	}

	shards := []pbm.Shard{{ID: im.SetName}}
	if im.IsSharded() {
		s, err := cn.GetShards()
		if err != nil {
			log.Fatalf("Error: get shards: %v", err)
		}
		shards = append(shards, s...)
	}

	ts, err := cn.ClusterTime()
	if err != nil {
		log.Fatalf("Read cluster time: %v", err)
	}

	now := time.Now().Unix()
	var pitrList string
	for _, s := range shards {
		if on {
			l, err := cn.GetLockData(&pbm.LockHeader{Replset: s.ID}, pbm.LockCollection)
			if err == mongo.ErrNoDocuments {
				// !!! SHOW LOG
			}
			if err != nil {
				log.Fatalf("Error: get lock for shard '%s': %v", s.ID, err)
			}
			switch l.Type {
			default:
				log.Fatalf("Error: undefined state, has lock for %v op while PITR is on", l.Type)
			case pbm.CmdPITR:
				if l.Heartbeat.T+pbm.StaleFrameSec < ts.T {
					// !!! SHOW LOG
				}
			case pbm.CmdBackup: // backup is ok
			}
		}

		tlns, err := cn.PITRGetValidTimelines(s.ID, now)
		if err != nil {
			log.Fatalf("Error: get PITR timelines for %s replset: %v", s.ID, err)
		}

		if len(tlns) == 0 {
			continue
		}

		rsout := fmt.Sprintf("  %s:", s.ID)
		for _, tln := range tlns {
			rsout += fmt.Sprintf(" %s,", tln)
		}
		pitrList += rsout[:len(rsout)-1] + "\n"
	}
	if len(pitrList) > 0 {
		fmt.Printf("PITR:\n%s", pitrList)
	}
}

// func formatts(t uint32) string {
// 	return time.Unix(int64(t), 0).UTC().Format(time.RFC3339)
// }

func printBackupProgress(b pbm.BackupMeta, pbmClient *pbm.PBM) (string, error) {
	locks, err := pbmClient.GetLocks(&pbm.LockHeader{
		Type:       pbm.CmdBackup,
		BackupName: b.Name,
	}, pbm.LockCollection)

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
