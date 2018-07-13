package dumper

import (
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func TestWriteToFile(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		t.Errorf("Cannot create temporary file for testing: %s", err)
	}

	mi := &MongodumpInput{
		Host:    "localhost",
		Port:    "17001",
		Gzip:    false,
		Oplog:   false,
		Threads: 1,
		Writer:  tmpFile,
	}

	mdump, err := NewMongodump(mi)
	if err != nil {
		t.Fatal(err)
	}

	mdump.Start()
	err = mdump.Wait()
	tmpFile.Close()

	fi, err := os.Stat(tmpFile.Name())
	if err != nil {
		t.Error(err)
	}

	if fi.Size() == 0 {
		t.Errorf("Invalid mongodump size (0)")
	}

	// TODO: Check if file content is valid. How?
	if err = os.Remove(tmpFile.Name()); err != nil {
		t.Errorf("Cannot remove temporary file %s: %s", tmpFile.Name(), err)
	}
}

func diag(params ...interface{}) {
	if testing.Verbose() {
		log.Printf(params[0].(string), params[1:]...)
	}
}
