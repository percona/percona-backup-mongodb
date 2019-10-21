package main

import (
	"context"
	"fmt"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
)

func backup(cn *pbm.PBM, bcpName, compression string) (string, error) {
	stg, err := cn.GetStorage()
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	err = waitForStatus(ctx, cn, bcpName, pbm.StatusRunning)
	if err != nil {
		return "", err
	}

	storeString := "s3://"
	if stg.S3.EndpointURL != "" {
		storeString += stg.S3.EndpointURL + "/"
	}
	storeString += stg.S3.Bucket

	return storeString, nil
}

func waitForStatus(ctx context.Context, cn *pbm.PBM, bcpName string, status pbm.Status) error {
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
			case status:
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
