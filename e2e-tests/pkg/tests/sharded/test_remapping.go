package sharded

import (
	"fmt"
	"log"
	"strings"

	"github.com/percona/percona-backup-mongodb/v2/pbm"
)

type RemappingEnvironment struct {
	Donor     *Cluster
	Recipient *Cluster
	Remapping map[string]string
}

func (re *RemappingEnvironment) prepareRestoreOptions(typ pbm.BackupType) []string {
	var remappings []string
	if typ == pbm.PhysicalBackup || len(re.Remapping) == 0 {
		return []string{}
	}

	for to, from := range re.Remapping {
		remappings = append(remappings, fmt.Sprintf("%s=%s", to, from))
	}
	return []string{"--replset-remapping", strings.Join(remappings, ",")}
}

func (re *RemappingEnvironment) BackupAndRestore(typ pbm.BackupType) {
	backup := re.Donor.LogicalBackup
	restore := re.Recipient.LogicalRestoreWithParams
	if typ == pbm.PhysicalBackup {
		backup = re.Donor.PhysicalBackup
		restore = re.Recipient.PhysicalRestoreWithParams
	}

	checkData := re.DataChecker()

	bcpName := backup()
	re.Donor.BackupWaitDone(bcpName)

	// to be sure the backup didn't vanish after the resync
	// i.e. resync finished correctly
	log.Println("resync backup list")
	err := re.Recipient.mongopbm.StoreResync()
	if err != nil {
		log.Fatalln("Error: resync backup lists:", err)
	}

	restore(bcpName, re.prepareRestoreOptions(typ))
	checkData()
}

func (re *RemappingEnvironment) DataChecker() (check func()) {
	hashes1 := make(map[string]map[string]string)
	for name, s := range re.Donor.shards {
		h, err := s.DBhashes()
		if err != nil {
			log.Fatalf("get db hashes %s: %v\n", name, err)
		}
		log.Printf("current Donor %s db hash %s\n", name, h["_all_"])
		hashes1[name] = h
	}

	return func() {
		log.Println("Checking restored backup with remapping")

		for name, s := range re.Recipient.shards {
			if donorName, ok := re.Remapping[name]; ok {
				h, err := s.DBhashes()
				if err != nil {
					log.Fatalf("get db hashes %s: %v\n", name, err)
				}
				if hashes1[donorName]["_all_"] != h["_all_"] {
					log.Fatalf(
						"%s: hashes don't match. before %s now %s",
						name, hashes1[donorName]["_all_"], h["_all_"],
					)
				}
			} else {
				log.Fatalf("%s: cannot find appropriate mapping in: %v", name, re.Remapping)
			}
		}
	}
}
