package restore

import (
	"testing"
)

func TestRestore(t *testing.T) {

	input := &MongoRestoreInput{
		// this file was generated with the dump pkg
		Archive: "testdata/dump_test.000622955",
		DryRun:  true,
		Host:    "localhost",
		Port:    "17001",
		Gzip:    false,
		Oplog:   false,
		Threads: 1,
		Reader:  nil,
	}

	r, err := NewMongoRestore(input)
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
