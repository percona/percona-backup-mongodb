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
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/hashicorp/go-multierror"
	"github.com/kr/pretty"
	"github.com/percona/percona-backup-mongodb/bsonfile"
	"github.com/percona/percona-backup-mongodb/internal/testutils"
)

type mockBSONReader struct {
	index int
	docs  []bson.M
}

func (m *mockBSONReader) ReadNext() ([]byte, error) {
	return []byte{}, nil
}

func (m *mockBSONReader) UnmarshalNext(dest interface{}) error {
	m.index++
	if m.index >= len(m.docs) {
		return io.EOF
	}
	pretty.Println(m.docs[m.index])
	buff, err := bson.Marshal(m.docs[m.index])
	if err != nil {
		return err
	}
	if err := bson.Unmarshal(buff, dest); err != nil {
		return err
	}

	return nil
}

func newMockBSONReader(docs []bson.M) *mockBSONReader {
	return &mockBSONReader{
		index: -1,
		docs:  docs,
	}
}

func TestApplyCreateCollection(t *testing.T) {
	docs := []bson.M{
		{
			"v":  int(2),
			"op": "c",
			"ns": "test.$cmd",
			"ts": bson.MongoTimestamp(6688736102603292686),
			"t":  int64(1),
			"h":  int64(2385982676716983621),
			"ui": bson.Binary{
				Kind: 0x4,
				Data: []byte{0xa6, 0xcb, 0xa6, 0xd5, 0xb0, 0x3, 0x4d, 0xab, 0x8b, 0x3, 0x96, 0xe9, 0x81, 0xc2, 0x9b, 0x48},
			},
			"wall": time.Now(),
			"o": bson.M{
				"create":          "testcol_00001",
				"autoIndexId":     bool(true),
				"validationLevel": "strict",
				"idIndex": bson.M{
					"ns": "test.testcol_00001",
					"v":  int(2),
					"key": bson.M{
						"_id": int(1),
					},
					"name": "_id_",
				},
			},
		},
		{
			"ts": bson.MongoTimestamp(6688736102603292690),
			"ns": "test.$cmd",
			"o": bson.M{
				"validationLevel": "strict",
				"idIndex": bson.M{
					"ns": "test.testcol_00002",
					"v":  int(2),
					"key": bson.M{
						"_id": int(1),
					},
					"name": "_id_",
				},
				"create":      "testcol_00002",
				"autoIndexId": bool(true),
			},
			"wall": time.Now(),
			"t":    int64(1),
			"h":    int64(4227177456625961085),
			"v":    int(2),
			"op":   "c",
			"ui": bson.Binary{
				Kind: 0x4,
				Data: []byte{0xf, 0x67, 0x4, 0x2f, 0x99, 0x17, 0x41, 0x8, 0xa0, 0x68, 0xe9, 0x51, 0x75, 0xc, 0xac, 0x3c},
			},
		},
	}

	bsonReader := newMockBSONReader(docs)

	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		t.Fatalf("Cannot connect to primary: %s", err)
	}

	dbname := "test"
	testColsPrefix := "testcol_"

	db := session.DB(dbname)

	if err := cleanup(db, testColsPrefix); err != nil {
		t.Fatalf("Cannot clean up %s database before starting: %s", dbname, err)
	}

	oa, err := NewOplogApply(session, bsonReader)
	if err != nil {
		t.Errorf("Cannot instantiate the oplog applier: %s", err)
	}

	// If you need to debug and see all failed operations output
	oa.ignoreErrors = true

	if err := oa.Run(); err != nil {
		t.Errorf("Error while running the oplog applier: %s", err)
	}
}

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

	dbname := "test"
	testColsPrefix := "testcol_"
	maxCollections := 10
	maxDocs := 50

	db := session.DB(dbname)

	if err := cleanup(db, testColsPrefix); err != nil {
		t.Fatalf("Cannot clean up %s database before starting: %s", dbname, err)
	}

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
