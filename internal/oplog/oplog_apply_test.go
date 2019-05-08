package oplog

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/hashicorp/go-multierror"
	"github.com/percona/percona-backup-mongodb/bsonfile"
	"github.com/percona/percona-backup-mongodb/internal/testutils"
)

func TestBasicApplyLog(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "example.bson.")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if testing.Verbose() {
		fmt.Printf("Dumping the oplog into %q\n", tmpfile.Name())
	}

	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		t.Fatalf("Cannot connect to primary: %s", err)
	}

	fmt.Println("0")
	dbname := "test"
	testColsPrefix := "testcol_"
	maxCollections := 10
	maxDocs := 50

	db := session.DB(dbname)

	if err := cleanup(db, testColsPrefix); err != nil {
		t.Fatalf("Cannot clean up %s database before starting: %s", dbname, err)
	}
	fmt.Println("0.1")

	ot, err := Open(session)
	if err != nil {
		t.Fatalf("Cannot instantiate the oplog tailer: %s", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Start tailing the oplog in background while some records are being generated
	go func() {
		if _, err := io.Copy(tmpfile, ot); err != nil {
			t.Errorf("Error while tailing the oplog: %s", err)
		}
		wg.Done()
	}()

	wg2 := &sync.WaitGroup{}
	validationLevels := []string{"strict", "moderate", "off"}
	for cc := 0; cc < maxCollections; cc++ {
		for disableIDIndex := 0; disableIDIndex < 2; disableIDIndex++ {
			for capped := 0; capped < 2; capped++ {
				for validationLevel := 0; validationLevel < 3; validationLevel++ {
					vLevel := validationLevel
					isCapped := capped
					dii := disableIDIndex
					cname := fmt.Sprintf("%s%05d", testColsPrefix, cc)
					wg2.Add(1)
					go func() {
						fmt.Printf("cname: %s\n", cname)
						col := session.Clone().DB(dbname).C(cname)
						var maxBytes, maxDocs int
						if isCapped != 0 {
							maxBytes = 10000 + int(rand.Int63n(1000))
							maxDocs = 500 + int(rand.Int63n(100))
						}

						cInfo := &mgo.CollectionInfo{
							DisableIdIndex:  dii == 0,
							ForceIdIndex:    dii != 0,
							Capped:          isCapped == 0,
							ValidationLevel: validationLevels[vLevel],
							MaxDocs:         maxDocs,
							MaxBytes:        maxBytes,
						}
						if err1 := col.Create(cInfo); err != nil {
							err = multierror.Append(err, err1)
						}

						for i := 0; i < maxDocs; i++ {
							rec := bson.M{"id": i, "name": fmt.Sprintf("name_%03d", i)}
							if err1 := col.Insert(rec); err != nil {
								err = multierror.Append(err, err1)
							}
						}
						fmt.Printf("%s done\n", cname)
						wg2.Done()
					}()
				}
			}
		}
	}
	wg2.Wait()

	ot.Close()
	fmt.Println("1")

	wg.Wait()
	tmpfile.Close()

	if err := cleanup(db, testColsPrefix); err != nil {
		t.Fatalf("Cannot clean up %s database before applying the oplog: %s", dbname, err)
	}

	n, _, err := count(db, testColsPrefix)
	if err != nil {
		t.Errorf("Cannot verify collections have been dropped before applying the oplog: %s", err)
	}
	if n != 0 {
		t.Fatalf("Collections %s* already exists. Cannot test oplog apply", testColsPrefix)
	}

	// Open the oplog file we just wrote and use it as a reader for the oplog apply
	reader, err := bsonfile.OpenFile(tmpfile.Name())
	if err != nil {
		t.Errorf("Cannot open oplog dump file %q: %s", tmpfile.Name(), err)
		return
	}

	// Replay the oplog
	oa, err := NewOplogApply(session, reader)
	if err != nil {
		t.Errorf("Cannot instantiate the oplog applier: %s", err)
	}

	if err := oa.Run(); err != nil {
		t.Errorf("Error while running the oplog applier: %s", err)
	}

	n, d, err := count(db, testColsPrefix)
	if err != nil {
		t.Errorf("Cannot verify collections have been created by the oplog: %s", err)
	}
	if n != maxCollections {
		t.Fatalf("Invalid collections count after aplplying the oplog. Want %d, got %d", maxCollections, n)
	}
	wantDocs := maxCollections * maxDocs
	if d != wantDocs {
		t.Fatalf("Invalid documents count after aplplying the oplog. Want %d, got %d", wantDocs, d)
	}

	if err := cleanup(db, testColsPrefix); err != nil {
		t.Fatalf("Cannot clean up %s database after testing: %s", dbname, err)
	}
}

func cleanup(db *mgo.Database, testColsPrefix string) error {
	// Clean up before starting
	colNames, err := db.CollectionNames()
	if err != nil {
		return fmt.Errorf("Cannot list collections in %q database", err)
	}
	for _, cname := range colNames {
		fmt.Println(cname)
		if strings.HasPrefix(cname, testColsPrefix) {
			if err := db.C(cname).DropCollection(); err != nil {
				return fmt.Errorf("Cannot drop %s collection: %s", cname, err)
			}
		}
	}
	return nil
}

func count(db *mgo.Database, testColsPrefix string) (int, int, error) {
	var totalCols, totalDocs int
	colNames, err := db.CollectionNames()
	if err != nil {
		return 0, 0, fmt.Errorf("Cannot list collections in %q database", err)
	}
	for _, cname := range colNames {
		if strings.HasPrefix(cname, testColsPrefix) {
			n, err := db.C(cname).Count()
			if err != nil {
				return 0, 0, fmt.Errorf("Cannot count documents in collection %s: %s", cname, err)
			}
			totalCols++
			totalDocs += n
		}
	}
	return totalCols, totalDocs, nil
}
