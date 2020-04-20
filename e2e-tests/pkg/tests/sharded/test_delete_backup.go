package sharded

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"strings"

	"github.com/minio/minio-go"
	"github.com/percona/percona-backup-mongodb/pbm"
	"gopkg.in/yaml.v2"
)

func (c *Cluster) BackupDelete(storage string) {
	checkData := c.DataChecker()

	backups := make([]string, 5)
	for i := 0; i < len(backups); i++ {
		bcpName := c.Backup()
		log.Println("doing backup:", bcpName)
		c.BackupWaitDone(bcpName)
		backups[i] = bcpName
	}

	c.printBcpList()

	log.Println("delete backup", backups[4])
	_, err := c.pbm.RunCmd("pbm", "delete-backup", "-f", backups[4])
	if err != nil {
		log.Fatalf("Error: delete backup %s: %v", backups[4], err)
	}

	c.printBcpList()

	log.Println("delete backups older than", backups[3])
	_, err = c.pbm.RunCmd("pbm", "delete-backup", "-f", "--older-than", backups[3])
	if err != nil {
		log.Fatalf("Error: delete backups older than %s: %v", backups[3], err)
	}

	c.printBcpList()

	log.Println("should be only backup", backups[3])
	checkNoFiles(backups[3], storage)

	blist, err := c.mongopbm.BackupsList(0)
	if err != nil {
		log.Fatalln("Error: get backups list", err)
	}

	if len(blist) != 1 || blist[0].Name != backups[3] {
		log.Fatalf("Error: wrong backups list. Should has been left only backup %s. But have:\n%v", backups[3], blist)
	}

	log.Println("trying to restore from", backups[3])
	c.DeleteBallast()
	c.Restore(backups[3])
	checkData()
}

const awsurl = "s3.amazonaws.com"

func checkNoFiles(exceptPrefix, conf string) {
	buf, err := ioutil.ReadFile(conf)
	if err != nil {
		log.Fatalln("Error: unable to read config file:", err)
	}

	var cfg pbm.Config
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

	mc, err := minio.NewWithRegion(endopintURL, stg.S3.Credentials.AccessKeyID, stg.S3.Credentials.SecretAccessKey, false, stg.S3.Region)
	if err != nil {
		log.Fatalln("Error: NewWithRegion:", err)
	}

	for object := range mc.ListObjects(stg.S3.Bucket, stg.S3.Prefix, true, nil) {
		if object.Err != nil {
			fmt.Println("Error: ListObjects: ", object.Err)
			continue
		}

		if !strings.HasPrefix(object.Key, exceptPrefix) {
			log.Fatalln("Error: delete lefover", object.Key)
		}
	}
}

func (c *Cluster) BackupNotDeleteRunning() {
	log.Println("starting backup")
	bcpName := c.Backup()

	log.Println("deleting backup", bcpName)
	o, err := c.pbm.RunCmd("pbm", "delete-backup", "-f", bcpName)
	if err == nil || !strings.Contains(err.Error(), "Error: Undable delete backup in running state") {
		list, lerr := c.pbm.RunCmd("pbm", "list")
		log.Fatalf("Error: running backup '%s' shouldn't be deleted.\nOutput: %s\nStderr:%s\nBackups list:\n%v\n%v", bcpName, o, err, list, lerr)
	}
	c.BackupWaitDone(bcpName)
}

func (c *Cluster) printBcpList() {
	listo, _ := c.pbm.RunCmd("pbm", "list")
	fmt.Printf("backup list:\n%s\n", listo)
}
