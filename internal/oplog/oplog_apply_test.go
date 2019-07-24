package oplog

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/kr/pretty"
	"github.com/percona/percona-backup-mongodb/bsonfile"
	"github.com/percona/percona-backup-mongodb/internal/testutils"
	"github.com/pkg/errors"
)

type oplogTestCases struct {
	collectionName string
	collectionInfo *mgo.CollectionInfo
	action         string
	maxDocs        int
	indexes        []mgo.Index
}

type mockBSONReader struct {
	index int
	docs  []bson.M
}

func (m *mockBSONReader) Close() error {
	return nil
}

func (m *mockBSONReader) Read(dest []byte) (int, error) {
	m.index++
	if m.index >= len(m.docs) {
		return 0, io.EOF
	}

	buf, err := bson.Marshal(m.docs[m.index])
	copy(dest, buf)
	return len(buf), err
}

func (m *mockBSONReader) ReadNext() ([]byte, error) {
	m.index++
	if m.index >= len(m.docs) {
		return nil, io.EOF
	}

	return bson.Marshal(m.docs[m.index])
}

func (m *mockBSONReader) UnmarshalNext(dest interface{}) error {
	m.index++
	if m.index >= len(m.docs) {
		return io.EOF
	}

	buff, err := bson.Marshal(m.docs[m.index])
	if err != nil {
		return errors.Wrap(err, "cannot marshal to unmarshal")
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

func TestIgnoreSystemCollections(t *testing.T) {
	docs := []bson.M{
		{
			"ts": bson.MongoTimestamp(6699345900185059329),
			"h":  int64(1603488249581412698),
			"op": "i",
			"ns": "config.system.sessions",
			"t":  int64(1),
			"v":  int(2),
			"ui": bson.Binary{
				Kind: 0x4,
				Data: []byte{0x1, 0x69, 0xc9, 0x79, 0xb4, 0x6e, 0x42, 0xcf, 0x87, 0x5b, 0x6b, 0xab, 0xfd, 0x89, 0xa4, 0x51},
			},
			"wall": time.Now().Add(-1 * time.Minute),
			"o": bson.M{
				"_id": bson.M{
					"id": bson.Binary{
						Kind: 0x4,
						Data: []byte{0xb7, 0xa1, 0x43, 0xd8, 0xf4, 0x54, 0x43, 0xb3, 0xad, 0xc5, 0x9c, 0xbd, 0x21, 0x41, 0xba, 0x44},
					},
					"uid": []byte{0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
						0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55},
				},
				"lastUse": time.Now().Add(-1 * time.Minute),
			},
		},
		{
			"t":  1,
			"ns": "config.cache.collections",
			"o2": bson.M{
				"_id": "ycsb_test2.usertable",
			},
			"wall": time.Now(),
			"ts":   bson.MongoTimestamp(time.Now().Unix()),
			"h":    int64(-2273244757254710743),
			"v":    2,
			"op":   "u",
			"ui": bson.Binary{
				Kind: 0x4,
				Data: []byte{0x1, 0x69, 0xc9, 0x79, 0xb4, 0x6e, 0x42, 0xcf, 0x87, 0x5b, 0x6b, 0xab, 0xfd, 0x89, 0xa4, 0x52},
			},
			"o": bson.M{
				"$v":   1,
				"$set": bson.M{"refreshing": true},
			},
		},
	}

	bsonReader := newMockBSONReader(docs)

	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		t.Fatalf("Cannot connect to primary: %s", err)
	}

	oa, err := NewOplogApply(session, bsonReader)
	if err != nil {
		t.Errorf("Cannot instantiate the oplog applier: %s", err)
	}

	// If you need to debug and see all failed operations output
	oa.ignoreErrors = false

	if err := oa.Run(); err != nil {
		t.Errorf("Error while running the oplog applier: %s", err)
	}
}

func TestApplyLogFromFile(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		t.Fatalf("Cannot connect to primary: %s", err)
	}
	defer session.Close()

	testutils.CleanupDatabases(session, nil)

	oplogFile := "testdata/dump.oplog"
	// Open the oplog file we just wrote and use it as a reader for the oplog apply
	reader, err := bsonfile.OpenFile(oplogFile)
	if err != nil {
		t.Errorf("Cannot open oplog dump file %q: %s", oplogFile, err)
		return
	}

	oa, err := NewOplogApply(session, reader)
	if err != nil {
		t.Errorf("Cannot instantiate the oplog applier: %s", err)
	}

	if err := oa.Run(); err != nil {
		t.Errorf("Error while running the oplog applier: %s", err)
	}
}

func TestBasicApplyLog(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "example.bson.")
	if err != nil {
		log.Fatal(err)
	}
	if os.Getenv("KEEP_DATA") == "" {
		defer os.Remove(tmpfile.Name())
	}

	if testing.Verbose() {
		fmt.Printf("Dumping the oplog into %q\n", tmpfile.Name())
	}

	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		t.Fatalf("Cannot connect to primary: %s", err)
	}

	if err := testutils.CleanupDatabases(session, nil); err != nil {
		t.Fatalf("cannot cleanup the databases before starting the test: %s", err)
	}

	ot, err := Open(session)
	if err != nil {
		t.Fatalf("Cannot instantiate the oplog tailer: %s", err)
	}
	if err := ot.WaitUntilFirstDoc(); err != nil {
		t.Fatalf("Cannot get first doc from oplog tailer: %s", err)
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

	dbname := "test"
	index := mgo.Index{
		Key:         []string{"f1", "-f2"},
		Unique:      true,
		Background:  true, // See notes.
		Sparse:      true,
		ExpireAfter: 5 * time.Minute,
		Name:        "this_is_my_index",
		Collation: &mgo.Collation{
			Locale:          "fr",
			CaseFirst:       "off",
			Strength:        3,
			Alternate:       "non-ignorable",
			MaxVariable:     "punct",
			Normalization:   false,
			CaseLevel:       false,
			NumericOrdering: false,
			Backwards:       false,
		},
	}

	testCases := []oplogTestCases{
		{
			collectionName: "test_col001",
			collectionInfo: &mgo.CollectionInfo{
				DisableIdIndex: false,
				ForceIdIndex:   true,
				Capped:         false,
			},
			action:  "create",
			maxDocs: 10,
			indexes: nil,
		},
		{
			collectionName: "test_col002",
			collectionInfo: &mgo.CollectionInfo{
				DisableIdIndex: false,
				ForceIdIndex:   true,
				Capped:         true,
				MaxDocs:        100,
				MaxBytes:       1E6,
			},
			action:  "create",
			maxDocs: 10,
			indexes: nil,
		},
		{
			collectionName: "test_complex_index",
			collectionInfo: &mgo.CollectionInfo{},
			action:         "index",
			indexes:        []mgo.Index{index},
		},
		{
			collectionName: "drop_col",
			collectionInfo: &mgo.CollectionInfo{},
			action:         "drop_col",
		},
	}

	db := session.DB(dbname)
	generateOplogData(t, db, testCases)

	ot.Close()

	wg.Wait()
	tmpfile.Close()

	if err := testutils.CleanupDatabases(session, nil); err != nil {
		t.Fatalf("cannot cleanup the databases before starting the test: %s", err)
	}

	// Open the oplog file we just wrote and use it as a reader for the oplog apply
	reader, err := bsonfile.OpenFile(tmpfile.Name())
	if err != nil {
		t.Errorf("Cannot open oplog dump file %q: %s", tmpfile.Name(), err)
		return
	}

	session2, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		t.Fatalf("Cannot connect to primary: %s", err)
	}
	// Replay the oplog
	oa, err := NewOplogApply(session2, reader)
	if err != nil {
		t.Errorf("Cannot instantiate the oplog applier: %s", err)
	}

	//oa.ignoreErrors = true

	if err := oa.Run(); err != nil {
		t.Errorf("Error while running the oplog applier: %s", err)
	}

	testGeneratedOplogData(t, db, testCases)

}

func generateOplogData(t *testing.T, db *mgo.Database, testCases []oplogTestCases) {
	for _, tc := range testCases {
		if tc.action == "create" {
			if err := db.C(tc.collectionName).Create(tc.collectionInfo); err != nil {
				t.Fatalf("Cannot create capped collection %s with info: %+v: %s",
					tc.collectionName, tc.collectionInfo, err)
			}
		}

		// generate some docs and remove some of them so we have inserts and deletes in the oplog
		if tc.maxDocs > 0 {
			for j := 0; j < tc.maxDocs; j++ {
				if err := db.C(tc.collectionName).Insert(bson.M{"f1": j, "f2": tc.maxDocs - j}); err != nil {
					t.Errorf("Cannot insert document # %d into collection %s: %s", j, tc.collectionName, err)
				}
			}
			if !tc.collectionInfo.Capped {
				if _, err := db.C(tc.collectionName).RemoveAll(bson.M{"f1": bson.M{"$gte": tc.maxDocs / 3}}); err != nil {
					t.Errorf("Cannot remove some documents: %s", err)
				}
			}
		}

		// if it is a drop_col test, insert a document just to force the create collection if not exists
		// and drop the collection
		if tc.action == "drop_col" {
			if err := db.C(tc.collectionName).Insert(bson.M{"f1": 33, "f2": 34}); err != nil {
				t.Errorf("Cannot create the new_index collection and insert a doc: %s", err)
			}
			if err := db.C(tc.collectionName).DropCollection(); err != nil {
				t.Errorf("Cannot drop the new_index collection")
			}
		}

		if tc.action == "index" {
			db.Session.ResetIndexCache()
			db.Session.Refresh()
			for _, index := range tc.indexes {
				pretty.Println(index)
				if err := db.C(tc.collectionName).EnsureIndex(index); err != nil {
					t.Errorf("Cannot create index for testing")
				}
			}
		}
	}
}

func testGeneratedOplogData(t *testing.T, db *mgo.Database, testCases []oplogTestCases) {
	for _, tc := range testCases {
		if tc.action == "create" {
			if !colExists(db, tc.collectionName) {
				t.Errorf("Collection %s doesn't exists", tc.collectionName)
				continue
			}

			if tc.maxDocs > 0 {
				n, err := db.C(tc.collectionName).Count()
				if err != nil {
					t.Errorf("Cannot count the # of documents in %s collection: %s", tc.collectionName, err)
				}
				wantDocsCount := int(tc.maxDocs / 3)
				if tc.collectionInfo.Capped {
					wantDocsCount = tc.maxDocs // documents cannot be removed from capped collections
				}
				if n != wantDocsCount {
					t.Errorf("invalid # of documents for collection %s. Want %d, got %d", tc.collectionName,
						wantDocsCount, n)
				}
			}
			continue
		}

		if tc.action == "drop_col" {
			if colExists(db, tc.collectionName) {
				t.Errorf("Collection %s was not dropped", tc.collectionName)
			}
			continue
		}

		if tc.action == "index" {
			db.Session.ResetIndexCache()
			db.Session.Refresh()
			idxs, err := db.C(tc.collectionName).Indexes()
			if err != nil {
				t.Errorf("Cannot get collection %q indexes: %s", tc.collectionName, err)
			}
			if len(idxs) != len(tc.indexes)+1 { // +1 because _id is generated automatically
				t.Errorf("Invalid number of indexes for collection %s: got %d want %d", tc.collectionName,
					len(idxs), len(tc.indexes))
			}
		}
	}
}

func TestComplexIndex(t *testing.T) {
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

	db := session.DB(dbname)

	if err := cleanup(db, ""); err != nil {
		t.Fatalf("Cannot clean up database before starting: %s", err)
	}

	ot, err := Open(session)
	if err != nil {
		t.Fatalf("Cannot instantiate the oplog tailer: %s", err)
	}
	if err := ot.WaitUntilFirstDoc(); err != nil {
		t.Fatalf("Cannot get first doc from oplog tailer: %s", err)
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

	if err := ot.WaitUntilFirstDoc(); err != nil {
		t.Fatalf("Error while waiting for first doc: %s", err)
	}

	complexIndexCol := "complex_idx"
	index := mgo.Index{
		Key:         []string{"f1", "-f2"},
		Unique:      true,
		Background:  true, // See notes.
		Sparse:      true,
		ExpireAfter: 5 * time.Minute,
		Name:        "this_is_my_index",
		Collation: &mgo.Collation{
			Locale:          "fr",
			CaseFirst:       "off",
			Strength:        3,
			Alternate:       "non-ignorable",
			MaxVariable:     "punct",
			Normalization:   false,
			CaseLevel:       false,
			NumericOrdering: false,
			Backwards:       false,
		},
	}
	if err := db.C(complexIndexCol).EnsureIndex(index); err != nil {
		t.Errorf("Cannot create index for testing")
	}

	ot.Close()
	wg.Wait()
	tmpfile.Close()

	var idxs []mgo.Index

	if err := db.C(complexIndexCol).DropCollection(); err != nil {
		t.Fatalf("Cannot drop %s: %s", complexIndexCol, err)
	}

	if err := cleanup(db, ""); err != nil {
		t.Fatalf("Cannot clean up %s database before applying the oplog: %s", dbname, err)
	}
	session.Refresh()
	session.ResetIndexCache()

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

	waitCount := 0

	for {
		db.Session.ResetIndexCache()
		db.Session.Refresh()
		idxs, err = db.C(complexIndexCol).Indexes()
		if err != nil {
			t.Errorf("Cannot get collection %q indexes: %s", complexIndexCol, err)
		}
		if len(idxs) > 1 {
			break
		}
		waitCount++
		if waitCount > 2 {
			break
		}
		time.Sleep(5 * time.Second)
	}

	if len(idxs) < 2 {
		t.Errorf("Invalid number of indexes for collection %s: %d < 1", complexIndexCol, len(idxs))
	} else if !reflect.DeepEqual(index, idxs[1]) {
		t.Errorf("Invalid index generated by optlog apply: %s", pretty.Diff(index, idxs[1]))
	}

	if err := cleanup(db, ""); err != nil {
		t.Fatalf("Cannot clean up %s database after testing: %s", dbname, err)
	}
}

func TestIndexes(t *testing.T) {
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
	colName := "col001"
	idxName := "idx001"

	db := session.DB(dbname)

	if err := cleanup(db, colName); err != nil {
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

	if err := ot.WaitUntilFirstDoc(); err != nil {
		t.Fatalf("Error while waiting for first doc: %s", err)
	}

	info := &mgo.CollectionInfo{
		DisableIdIndex: false,
		ForceIdIndex:   true,
		Capped:         false,
	}

	if err := db.C(colName).Create(info); err != nil {
		t.Fatalf("Cannot create collection %s with info: %+v: %s", colName, info, err)
	}

	if err := db.C(colName).Insert(bson.M{"f1": 33, "f2": 34}); err != nil {
		t.Errorf("Cannot create the new_index collection and insert a doc: %s", err)
	}
	if err := db.C(colName).DropCollection(); err != nil {
		t.Errorf("Cannot drop the new_index collection")
	}

	index := mgo.Index{
		Key:        []string{"f1", "-f2"},
		Unique:     true,
		DropDups:   true,
		Background: true,
		Sparse:     true,
		Name:       idxName + "_1", // DropIndex will add "_1"
	}

	if err := db.C(colName).EnsureIndex(index); err != nil {
		t.Errorf("Cannot create index for testing")
	}

	if err := db.C(colName).DropIndex(idxName); err != nil {
		t.Errorf("Cannot remove index %q: %s", idxName, err)
	}

	ot.Close()

	wg.Wait()
	tmpfile.Close()

	if err := cleanup(db, colName); err != nil {
		t.Fatalf("Cannot clean up %s database before applying the oplog: %s", dbname, err)
	}

	n, _, err := count(db, colName)
	if err != nil {
		t.Errorf("Cannot verify collections have been dropped before applying the oplog: %s", err)
	}

	if n != 0 {
		t.Fatalf("Collections %s* already exists. Cannot test oplog apply", colName)
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

	if !colExists(db, colName) {
		t.Errorf("Collection %s does not exists", colName)
	}

	idxs, err := db.C(colName).Indexes()
	if err != nil {
		t.Errorf("Cannot list indexes for collection %q: %s", colName, err)
	}

	for _, idx := range idxs {
		if idx.Name == idxName {
			t.Errorf("Index %q still exist", idx.Name)
		}
	}

	if err := cleanup(db, colName); err != nil {
		t.Fatalf("Cannot clean up %s database after testing: %s", dbname, err)
	}

}

func TestConvertToInsert(t *testing.T) {
	filename := "testdata/example.bson.316738284"
	reader, err := bsonfile.OpenFile(filename)
	if err != nil {
		t.Errorf("Cannot open oplog dump file %q: %s", filename, err)
		return
	}

	want := bson.D{
		{
			Name:  "ts",
			Value: bson.MongoTimestamp(6716547643637497858),
		},
		{
			Name:  "h",
			Value: int64(-4724916785628486220),
		},
		{
			Name:  "v",
			Value: int(2),
		},
		{
			Name:  "op",
			Value: "i",
		},
		{
			Name:  "ns",
			Value: "test.system.indexes",
		},
		{
			Name: "ui",
			Value: bson.Binary{
				Kind: 0x4,
				Data: []byte{0xf1, 0x5e, 0x9c, 0xf0, 0x8e, 0xeb, 0x4d, 0x60, 0xaf, 0xe6, 0xc5, 0x4c, 0x98, 0xe8, 0xf, 0xb0},
			},
		},
		{
			Name:  "wall",
			Value: time.Date(2019, 07, 22, 17, 55, 11, int(139*time.Millisecond), time.UTC),
		},
		{
			Name: "o",
			Value: bson.D{
				{
					Name:  "v",
					Value: int(2),
				},
				{
					Name:  "unique",
					Value: bool(true),
				},
				{
					Name: "key",
					Value: bson.D{
						{
							Name:  "f1",
							Value: int(1),
						},
						{
							Name:  "f2",
							Value: int(-1),
						},
					},
				},
				{
					Name:  "ns",
					Value: "test.test_complex_index",
				},
				{
					Name:  "name",
					Value: "this_is_my_index",
				},
				{
					Name:  "background",
					Value: bool(true),
				},
				{
					Name:  "sparse",
					Value: bool(true),
				},
				{
					Name:  "expireAfterSeconds",
					Value: int(300),
				},
				{
					Name: "collation",
					Value: bson.D{
						{
							Name:  "locale",
							Value: "fr",
						},
						{
							Name:  "caseLevel",
							Value: bool(false),
						},
						{
							Name:  "caseFirst",
							Value: "off",
						},
						{
							Name:  "strength",
							Value: int(3),
						},
						{
							Name:  "numericOrdering",
							Value: bool(false),
						},
						{
							Name:  "alternate",
							Value: "non-ignorable",
						},
						{
							Name:  "maxVariable",
							Value: "punct",
						},
						{
							Name:  "normalization",
							Value: bool(false),
						},
						{
							Name:  "backwards",
							Value: bool(false),
						},
						{
							Name:  "version",
							Value: "57.1",
						},
					},
				},
			},
		},
	}

	for {
		buf, err := reader.ReadNext()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Errorf("cannot read the next document: %s", err)
		}
		oplog := bson.D{}

		if err := bson.Unmarshal(buf, &oplog); err != nil {
			t.Errorf("cannot marshal the test oplog document: %s", err)
		}

		oplogMap := oplog.Map()
		o, ok := oplogMap["o"]
		if !ok {
			t.Errorf("there is no 'o' field")
			continue
		}

		if o.(bson.D)[0].Name == "createIndexes" {
			n, err := convertCreateIndexToIndexInsert(oplog)
			if err != nil {
				t.Errorf("cannot convert create index to an insert: %s", err)
			}
			if !reflect.DeepEqual(n, want) {
				t.Errorf("invalid generated insert command")
			}
		}
	}

}

func colExists(db *mgo.Database, name string) bool {
	cols, err := db.CollectionNames()
	if err != nil {
		return false
	}
	for _, col := range cols {
		if col == name {
			return true
		}
	}
	return false
}

func cleanup(db *mgo.Database, testColsPrefix string) error {
	// Clean up before starting
	if testColsPrefix == "" {
		return db.DropDatabase()
	}

	colNames, err := db.CollectionNames()
	if err != nil {
		return fmt.Errorf("Cannot list collections in %q database", err)
	}
	for _, cname := range colNames {
		if strings.HasPrefix(cname, testColsPrefix) {
			if err := db.C(cname).DropCollection(); err != nil {
				fmt.Printf("Cannot drop %s collection: %s", cname, err)
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
