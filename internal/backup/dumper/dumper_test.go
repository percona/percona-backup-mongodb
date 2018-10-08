package dumper

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/percona/mongodb-backup/internal/testutils"
)

var (
	keepS3Data     bool
	keepLocalFiles bool
)

func TestMain(m *testing.M) {
	flag.BoolVar(&keepS3Data, "keep-s3-data", false, "Do not delete S3 testing bucket and file")
	flag.BoolVar(&keepLocalFiles, "keep-local-files", false, "Do not files downloaded from the S3 bucket")
	flag.Parse()
	os.Exit(m.Run())
}

func TestWriteToFile(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "dump_test.")
	if err != nil {
		t.Errorf("Cannot create temporary file for testing: %s", err)
	}
	diag("Using temporary file %q", tmpFile.Name())

	host := strings.SplitN(testutils.GetMongoDBAddr(testutils.MongoDBShard1ReplsetName, "primary"), ":", 2)
	mi := &MongodumpInput{
		Host:     host[0],
		Port:     host[1],
		Username: testutils.MongoDBUser,
		Password: testutils.MongoDBPassword,
		Gzip:     false,
		Oplog:    false,
		Threads:  1,
		Writer:   tmpFile,
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

	if !keepLocalFiles {
		// TODO: Check if file content is valid. How?
		diag("Removing file %q", tmpFile.Name())
		if err = os.Remove(tmpFile.Name()); err != nil {
			t.Errorf("Cannot remove temporary file %s: %s", tmpFile.Name(), err)
		}
	}

}

func diag(params ...interface{}) {
	if testing.Verbose() {
		log.Printf(params[0].(string), params[1:]...)
	}
}
