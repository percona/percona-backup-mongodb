package client

import (
	"context"
	"os"
	"reflect"
	"testing"

	"github.com/kr/pretty"
	"github.com/percona/percona-backup-mongodb/internal/testutils"
	"github.com/percona/percona-backup-mongodb/internal/testutils/testenv"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
)

func TestValidateFilesystemStorage(t *testing.T) {
	input, err := buildInputParams()
	if err != nil {
		t.Fatalf("Cannot build agent's input params: %s", err)
	}
	c, err := NewClient(context.TODO(), input)
	msg := &pb.GetStorageInfo{
		StorageName: "local-filesystem",
	}

	stg, err := input.Storages.Get("local-filesystem")
	if err != nil {
		t.Fatalf("Cannot get S3 storage: %s", err)
	}

	wantFs := pb.ClientMessage{
		Version:  0,
		ClientId: "",
		Payload: &pb.ClientMessage_StorageInfo{
			StorageInfo: &pb.StorageInfo{
				Name:     "local-filesystem",
				Type:     "filesystem",
				Valid:    true,
				CanRead:  true,
				CanWrite: true,
				S3:       &pb.S3{},
				Filesystem: &pb.Filesystem{
					Path: stg.Filesystem.Path,
				},
			},
		},
	}

	info, err := c.processGetStorageInfo(msg)
	if err != nil {
		t.Errorf("Cannot process GetStorageInfo: %s", err)
	}
	if !reflect.DeepEqual(wantFs, info) {
		t.Errorf("Invalid info.\nWant:\n%s\nGot:\n%s", pretty.Sprint(wantFs), pretty.Sprint(info))
	}
}

func TestValidateS3Storage(t *testing.T) {
	input, err := buildInputParams()
	if err != nil {
		t.Fatalf("Cannot build agent's input params: %s", err)
	}
	c, err := NewClient(context.TODO(), input)
	msg := &pb.GetStorageInfo{
		StorageName: "s3-us-west",
	}

	stg, err := input.Storages.Get("s3-us-west")
	if err != nil {
		t.Fatalf("Cannot get S3 storage: %s", err)
	}

	wantFs := pb.ClientMessage{
		Version:  0,
		ClientId: "",
		Payload: &pb.ClientMessage_StorageInfo{
			StorageInfo: &pb.StorageInfo{
				Name:     "s3-us-west",
				Type:     "s3",
				Valid:    true,
				CanRead:  true,
				CanWrite: true,
				S3: &pb.S3{
					Region:      "us-west-2",
					EndpointUrl: "",
					Bucket:      stg.S3.Bucket,
				},
				Filesystem: &pb.Filesystem{},
			},
		},
	}

	info, err := c.processGetStorageInfo(msg)
	if err != nil {
		t.Errorf("Cannot process GetStorageInfo: %s", err)
	}
	if !reflect.DeepEqual(wantFs, info) {
		t.Errorf("Invalid info.\nWant:\n%s\nGot:\n%s", pretty.Sprint(wantFs), pretty.Sprint(info))
	}
}

func buildInputParams() (InputOptions, error) {
	port := testutils.MongoDBShard1PrimaryPort
	rs := testutils.MongoDBShard1ReplsetName

	di, err := testutils.DialInfoForPort(rs, port)
	if err != nil {
		return InputOptions{}, err
	}

	storages := testutils.TestingStorages()

	dbConnOpts := ConnectionOptions{
		Host:           testenv.MongoDBHost,
		Port:           port,
		User:           di.Username,
		Password:       di.Password,
		ReplicasetName: di.ReplicaSetName,
	}

	input := InputOptions{
		BackupDir:     os.TempDir(),
		DbConnOptions: dbConnOpts,
		GrpcConn:      nil,
		Logger:        nil,
		Storages:      storages,
	}

	return input, nil
}
