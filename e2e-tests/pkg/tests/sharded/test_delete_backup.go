package sharded

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/minio/minio-go"
	"github.com/percona/percona-backup-mongodb/pbm"
	"gopkg.in/yaml.v2"
)

type backupDelete struct {
	name string
	ts   time.Time
}

func (c *Cluster) BackupDelete(storage string) {
	// leftvers from the prev tests
	// the last backup shouldn't be deleted as it is a base for the PITR timeline
	bl, err := c.mongopbm.BackupsList(1)
	if err != nil {
		log.Fatalf("Error: get backups list: %v", err)
	}
	if len(bl) == 0 {
		log.Fatalln("Error: no backups, expected to have some")
	}

	left := map[string]struct{}{
		bl[0].Name: {},
	}

	checkData := c.DataChecker()

	backups := make([]backupDelete, 5)
	for i := 0; i < len(backups); i++ {
		ts := time.Now()
		time.Sleep(1 * time.Second)
		c.printBcpList()
		bcpName := c.Backup()
		backups[i] = backupDelete{
			name: bcpName,
			ts:   ts,
		}
		c.BackupWaitDone(bcpName)
	}

	c.printBcpList()

	log.Println("delete backup", backups[4].name)
	_, err = c.pbm.RunCmd("pbm", "delete-backup", "-f", backups[4].name)
	if err != nil {
		log.Fatalf("Error: delete backup %s: %v", backups[4].name, err)
	}

	log.Println("wait for delete")
	err = c.mongopbm.WaitConcurentOp(&pbm.LockHeader{Type: pbm.CmdDeleteBackup}, time.Minute*5)
	if err != nil {
		log.Fatalf("waiting for the delete: %v", err)
	}

	c.printBcpList()

	log.Printf("delete backups older than %s / %s \n", backups[3].name, backups[3].ts.Format("2006-01-02T15:04:05"))
	_, err = c.pbm.RunCmd("pbm", "delete-backup", "-f", "--older-than", backups[3].ts.Format("2006-01-02T15:04:05"))
	if err != nil {
		log.Fatalf("Error: delete backups older than %s: %v", backups[3].name, err)
	}
	log.Println("wait for delete")
	err = c.mongopbm.WaitConcurentOp(&pbm.LockHeader{Type: pbm.CmdDeleteBackup}, time.Minute*5)
	if err != nil {
		log.Fatalf("waiting for the delete: %v", err)
	}

	c.printBcpList()

	left[backups[3].name] = struct{}{}
	log.Println("should be only backup", left)
	checkArtefacts(storage, left)

	blist, err := c.mongopbm.BackupsList(0)
	if err != nil {
		log.Fatalln("Error: get backups list", err)
	}

	for _, b := range blist {
		if _, ok := left[b.Name]; !ok {
			log.Fatalf("Error: backup %s should be deleted", b.Name)
		}
	}

	log.Println("trying to restore from", backups[3])
	c.DeleteBallast()
	c.Restore(backups[3].name)
	checkData()
}

const awsurl = "s3.amazonaws.com"

// checkArtefacts checks if all backups artefacts removed
// except for the shouldStay
func checkArtefacts(conf string, shouldStay map[string]struct{}) {
	log.Println("check all artefacts deleted excepts backup's", shouldStay)
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
		if strings.Contains(object.Key, pbm.StorInitFile) || strings.Contains(object.Key, "/pbmPitr/") {
			continue
		}
		if object.Err != nil {
			fmt.Println("Error: ListObjects: ", object.Err)
			continue
		}

		var ok bool
		for b := range shouldStay {
			if strings.Contains(object.Key, b) {
				ok = true
				break
			}
		}
		if !ok {
			log.Fatalln("Error: failed to delete lefover", object.Key)
		}
	}
}

func (c *Cluster) BackupNotDeleteRunning() {
	bcpName := c.Backup()
	c.printBcpList()
	log.Println("deleting backup", bcpName)
	o, err := c.pbm.RunCmd("pbm", "delete-backup", "-f", bcpName)
	if err == nil || !strings.Contains(err.Error(), "unable to delete backup in running state") {
		list, lerr := c.pbm.RunCmd("pbm", "list")
		log.Fatalf("Error: running backup '%s' shouldn't be deleted.\nOutput: %s\nStderr:%v\nBackups list:\n%v\n%v", bcpName, o, err, list, lerr)
	}
	c.BackupWaitDone(bcpName)
	time.Sleep(time.Second * 2)
}

func (c *Cluster) printBcpList() {
	listo, _ := c.pbm.RunCmd("pbm", "list")
	fmt.Printf("backup list:\n%s\n", listo)
}
