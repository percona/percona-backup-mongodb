package restore

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/percona/percona-backup-mongodb/internal/reader"
	"github.com/percona/percona-backup-mongodb/internal/testutils"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
	"github.com/percona/percona-backup-mongodb/storage"
	"github.com/pkg/errors"
	"github.com/prometheus/common/log"
)

func TestRestore(t *testing.T) {
	host := strings.SplitN(testutils.GetMongoDBAddr(testutils.MongoDBShard1ReplsetName, "primary"), ":", 2)
	fakeStorage := storage.Storage{
		Type: "filesystem",
		Filesystem: storage.Filesystem{
			Path: filepath.Join(testutils.BaseDir(), "internal/restore/testdata/"),
		},
	}
	dumpFile := filepath.Join(testutils.BaseDir(), "internal/restore/testdata/2019-07-16T16:48:26Z_rs1.dump")
	oplogFile := "2019-07-16T16:48:26Z_rs1.oplog"
	pipeFile, err := ioutil.TempFile("", "pipe_")
	if err != nil {
		t.Fatalf("cannot get a temp file name for the named pipe: %s", err)
	}

	// stream the oplog file into the named pipe using our reader so we can stream it
	// from the local filesystem or S3.
	// Our reader is also able to handle gzipped files and ciphers (not really yet supported)
	if err := namedPipeWrite(fakeStorage, oplogFile, pipeFile.Name()); err != nil {
		t.Fatalf("cannot start the named pipe writer: %s", err)
	}

	input := &MongoRestoreInput{
		// this file was generated with the dump pkg. It has 1000 docs
		Archive:         dumpFile,
		OplogFile:       pipeFile.Name(),
		DryRun:          false,
		DropCollections: true,
		Host:            host[0],
		Port:            host[1],
		Username:        testutils.MongoDBUser,
		Password:        testutils.MongoDBPassword,
		Gzip:            false,
		Oplog:           false,
		Threads:         1,
		Reader:          nil,
	}

	r, err := NewMongoRestore(input, nil)
	if err != nil {
		t.Errorf("Cannot instantiate mongo restore instance: %s", err)
		t.FailNow()
	}

	if err := r.Start(); err != nil {
		t.Errorf("Cannot start restore: %s", err)
	}

	if err := r.Wait(); err != nil {
		t.Errorf("Error while trying to restore: %s", err)
	}
}

func namedPipeWrite(stg storage.Storage, input, output string) error {
	to, err := os.OpenFile(output, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		return errors.Wrapf(err, "cannot open output file %q", output)
	}
	defer to.Close()

	from, err := reader.MakeReader(input, stg, pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION,
		pb.Cypher_CYPHER_NO_CYPHER)
	if err != nil {
		return errors.Wrapf(err, "cannot open input file %q", input)
	}
	defer from.Close()

	// start writing data to the pipe in the background
	go func() {
		_, err := io.Copy(to, from)
		if err != nil {
			log.Errorf("errors while copying source to the pipe: %s", err)
		}
	}()

	return nil
}
