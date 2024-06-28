package sharded

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"gopkg.in/yaml.v2"

	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
)

func (c *Cluster) BackupCancellation(storage string) {
	bcpName := c.LogicalBackup()
	ts := time.Now()
	time.Sleep(1 * time.Second)
	c.printBcpList()
	log.Println("canceling backup", bcpName)
	o, err := c.pbm.RunCmd("pbm", "cancel-backup")
	if err != nil {
		log.Fatalf("Error: cancel backup '%s'.\nOutput: %s\nStderr:%v", bcpName, o, err)
	}

	time.Sleep(20 * time.Second)

	// checkNoBackupFiles(bcpName, storage)

	log.Println("check backup state")
	m, err := c.mongopbm.GetBackupMeta(context.TODO(), bcpName)
	if err != nil {
		log.Fatalf("Error: get metadata for backup %s: %v", bcpName, err)
	}

	if m.Status != defs.StatusCancelled {
		log.Fatalf("Error: wrong backup status, expect %s, got %v", defs.StatusCancelled, m.Status)
	}

	needToWait := defs.WaitBackupStart + time.Second - time.Since(ts)
	if needToWait > 0 {
		log.Printf("waiting for the lock to be released for %s", needToWait)
		time.Sleep(needToWait)
	}
}

//nolint:unused
func checkNoBackupFiles(backupName, conf string) {
	log.Println("check no artifacts left for backup", backupName)
	buf, err := os.ReadFile(conf)
	if err != nil {
		log.Fatalln("Error: unable to read config file:", err)
	}

	var cfg config.Config
	err = yaml.UnmarshalStrict(buf, &cfg)
	if err != nil {
		log.Fatalln("Error: unmarshal yaml:", err)
	}

	stg := cfg.Storage

	endopintURL := awsurl
	if stg.S3.EndpointURL != "" {
		eu, err := url.Parse(stg.S3.EndpointURL)
		if err != nil {
			log.Fatalln("Error: parse EndpointURL:", err)
		}
		endopintURL = eu.Host
	}

	mc, err := minio.New(endopintURL, &minio.Options{
		Creds: credentials.NewStaticV4(
			stg.S3.Credentials.AccessKeyID,
			stg.S3.Credentials.SessionToken,
			stg.S3.Credentials.SecretAccessKey),
		Secure: false,
		Region: stg.S3.Region,
	})
	if err != nil {
		log.Fatalln("Error: NewWithRegion:", err)
	}

	fileC := mc.ListObjects(context.Background(),
		stg.S3.Bucket, minio.ListObjectsOptions{
			Recursive: true,
			Prefix:    stg.S3.Prefix,
		})
	for object := range fileC {
		if object.Err != nil {
			fmt.Println("Error: ListObjects: ", object.Err)
			continue
		}

		if strings.Contains(object.Key, backupName) {
			log.Fatalln("Error: failed to delete lefover", object.Key)
		}
	}
}
