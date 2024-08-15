package sharded

import (
	"context"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awsS3 "github.com/aws/aws-sdk-go/service/s3"

	"gopkg.in/yaml.v2"

	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/storage/s3"
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

	ss, err := newS3Client(endopintURL, stg.S3.Region, &stg.S3.Credentials)
	if err != nil {
		log.Fatalf("create S3 client: %v", err)
	}

	res, err := ss.ListObjectsV2(&awsS3.ListObjectsV2Input{
		Bucket: &stg.S3.Bucket,
		Prefix: &stg.S3.Prefix,
	})
	if err != nil {
		log.Fatalf("list files on S3: %v", err)
	}

	for _, object := range res.Contents {
		s := object.String()
		if strings.Contains(s, backupName) {
			log.Fatalln("Error: failed to delete lefover", object.Key)
		}
	}
}

func newS3Client(uri, region string, creds *s3.Credentials) (*awsS3.S3, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:   &region,
		Endpoint: &uri,
		Credentials: credentials.NewStaticCredentials(
			creds.AccessKeyID,
			creds.SecretAccessKey,
			creds.SessionToken,
		),
	})
	if err != nil {
		return nil, errors.Wrap(err, "create AWS session")
	}

	return awsS3.New(sess), nil
}
