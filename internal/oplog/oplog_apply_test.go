package oplog

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"testing"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/bsonfile"
	"github.com/percona/mongodb-backup/internal/testutils"
	"github.com/percona/mongodb-backup/internal/testutils/db"
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

	session, err := mgo.DialWithInfo(db.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	ot, err := Open(session)
	if err != nil {
		t.Fatalf("Cannot instantiate the oplog tailer: %s", err)
	}

	dbname := "test"
	colname := "test_collection"
	docCount := 50

	db := session.DB(dbname)
	col := db.C(colname)

	// Clean up before starting
	col.DropCollection()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Start tailing the oplog in background while some records are being generated
	go func() {
		io.Copy(tmpfile, ot)
		wg.Done()
	}()

	for i := 0; i < docCount; i++ {
		rec := bson.M{"id": i, "name": fmt.Sprintf("name_%03d", i)}
		if err := col.Insert(rec); err != nil {
			t.Fatalf("Cannot insert row: %v\n%s", rec, err)
		}
	}

	n, err := col.Find(nil).Count()
	if err != nil {
		t.Errorf("Cannot get documents count: %s", err)
		return
	}

	if n != docCount {
		t.Errorf("Invalid document count. Want %d, got %d", docCount, n)
	}

	ot.Close()

	wg.Wait()
	tmpfile.Close()

	// Now, empty the collection and try to re-apply the oplog
	if _, err := col.RemoveAll(nil); err != nil {
		t.Errorf("Cannot drop the test collection %q: %s", colname, err)
		return
	}
	// Ensure the collection is empty
	n, err = col.Find(nil).Count()
	if err != nil {
		t.Errorf("Cannot get documents count: %s", err)
		return
	}

	if n != 0 {
		t.Errorf("Invalid document count after applying oplog. Want %d, got %d", docCount, n)
		return
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

	// Maybe this is not necessary but just to be completly sure, lets check the
	// documents we have in the collection after replaying the oplog, are the same
	// we wrote in the inserts above.
	iter := col.Find(nil).Iter()
	doc := bson.M{}
	i := 0
	for iter.Next(&doc) {
		gotID, ok := doc["id"]
		if !ok {
			t.Errorf("The returned document #%dis invalid. Missing 'id' field", i)
			break
		}

		gotName, ok := doc["name"]
		if !ok {
			t.Errorf("The returned document #%dis invalid. Missing 'name' field", i)
			break
		}

		if gotID.(int) != i {
			t.Errorf("Invalid id value for document #%d. Want: %d, got %d", i, i, doc["id"].(int))
		}

		wantName := fmt.Sprintf("name_%03d", i)
		if gotName.(string) != wantName {
			t.Errorf("Invalid name value for document #%d. Want: %q, got %q", i, wantName, gotName)
		}
		i++
	}

	if i != docCount {
		t.Errorf("Invalid document count after repalying the oplog. Want %d, got %d", docCount, i)
	}
}
