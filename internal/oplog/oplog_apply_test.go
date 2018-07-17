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

	session, err := mgo.Dial(dbUri)
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
		col.Insert(rec)
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

	// Now drop the collection and try to re-apply the oplog
	if err := col.DropCollection(); err != nil {
		t.Errorf("Cannot drop the test collection %q: %s", colname, err)
		return
	}
	n, err = col.Find(nil).Count()
	if err != nil {
		t.Errorf("Cannot get documents count: %s", err)
		return
	}

	if n != 0 {
		t.Errorf("Invalid document count. Want %d, got %d", docCount, n)
		return
	}

	reader, err := bsonfile.OpenFile(tmpfile.Name())
	if err != nil {
		t.Errorf("Cannot open oplog dump file %q: %s", tmpfile.Name(), err)
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
