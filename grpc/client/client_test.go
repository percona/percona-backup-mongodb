package client

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/kr/pretty"
	"github.com/percona/percona-backup-mongodb/internal/storage"
	"github.com/percona/percona-backup-mongodb/internal/testutils"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
	log "github.com/sirupsen/logrus"
)

func TestStorageInfo(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	bucket := fmt.Sprintf("pbm-test-bucket-%06d", rand.Int63n(999999))
	tmpDir := os.TempDir()
	minioEndpoint := os.Getenv("MINIO_ENDPOINT")

	st := &storage.Storages{
		Storages: map[string]storage.Storage{
			"s3-us-west": {
				Type: "s3",
				S3: storage.S3{
					Region: "us-west-2",
					Bucket: bucket,
					Credentials: storage.Credentials{
						AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
						SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
					},
				},
				Filesystem: storage.Filesystem{},
			},
			"local-filesystem": {
				Type: "filesystem",
				Filesystem: storage.Filesystem{
					Path: tmpDir,
				},
			},
			"minio": {
				Type: "s3",
				S3: storage.S3{
					Region:      "us-west-2",
					EndpointURL: minioEndpoint,
					Bucket:      bucket,
					Credentials: storage.Credentials{
						AccessKeyID:     os.Getenv("MINIO_ACCESS_KEY_ID"),
						SecretAccessKey: os.Getenv("MINIO_SECRET_ACCESS_KEY"),
					},
				},
				Filesystem: storage.Filesystem{},
			},
		},
	}

	input := InputOptions{
		BackupDir: tmpDir,
		Storages:  st,
		//	DbConnOptions ConnectionOptions
		//	DbSSLOptions  SSLOptions
		//	GrpcConn      *grpc.ClientConn
		//	Logger        *logrus.Logger
	}

	cl, err := NewClient(context.Background(), input)
	if err != nil {
		t.Fatalf("Cannot create a new client (node): %s", err)
	}

	sNames := []string{"s3-us-west", "local-filesystem", "minio"}
	for i, sName := range sNames {
		var valid, canRead, canWrite bool
		var err error
		storage, ok := st.Storages[sName]
		if !ok {
			t.Errorf("Storage %q doesn't exists", sName)
			continue
		}
		switch storage.Type {
		case "s3":
			valid, canRead, canWrite, err = cl.checkS3Access(st.Storages[sName].S3)
		case "filesystem":
			valid, canRead, canWrite, err = cl.checkFilesystemAccess(st.Storages[sName].Filesystem.Path)

		}
		if !valid {
			t.Errorf("Storage [%d] %q is invalid: %s\n", i, sName, err)
			continue
		}
		if !canRead {
			t.Errorf("Cannot read storage %q: %s\n", sName, err.Error())
		}
		if !canWrite {
			t.Errorf("Cannot write storage %q: %s\n", sName, err.Error())
		}
	}

	want := []*pb.ClientMessage{
		&pb.ClientMessage{
			Version:  0,
			ClientId: "",
			Payload: &pb.ClientMessage_StorageInfo{
				StorageInfo: &pb.StorageInfo{
					Name: "s3-us-west",
					Type: "s3",
					S3: &pb.S3{
						Region:               "us-west-2",
						EndpointUrl:          "",
						Bucket:               bucket,
						XXX_NoUnkeyedLiteral: struct{}{},
						XXX_unrecognized:     nil,
						XXX_sizecache:        0,
					},
					Filesystem:           &pb.Filesystem{},
					Valid:                false,
					CanRead:              false,
					CanWrite:             false,
					ErrorMsg:             "",
					XXX_NoUnkeyedLiteral: struct{}{},
					XXX_unrecognized:     nil,
					XXX_sizecache:        0,
				},
			},
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		},

		&pb.ClientMessage{
			Version:  0,
			ClientId: "",
			Payload: &pb.ClientMessage_StorageInfo{
				StorageInfo: &pb.StorageInfo{
					Name: "local-filesystem",
					Type: "filesystem",
					S3:   &pb.S3{},
					Filesystem: &pb.Filesystem{
						Path:                 tmpDir,
						XXX_NoUnkeyedLiteral: struct{}{},
						XXX_unrecognized:     nil,
						XXX_sizecache:        0,
					},
					Valid:                true,
					CanRead:              true,
					CanWrite:             true,
					ErrorMsg:             "",
					XXX_NoUnkeyedLiteral: struct{}{},
					XXX_unrecognized:     nil,
					XXX_sizecache:        0,
				},
			},
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		},
		&pb.ClientMessage{
			Version:  0,
			ClientId: "",
			Payload: &pb.ClientMessage_StorageInfo{
				StorageInfo: &pb.StorageInfo{
					Name: "minio",
					Type: "s3",
					S3: &pb.S3{
						Region:               "us-west-2",
						EndpointUrl:          minioEndpoint,
						Bucket:               bucket,
						XXX_NoUnkeyedLiteral: struct{}{},
						XXX_unrecognized:     nil,
						XXX_sizecache:        0,
					},
					Filesystem:           &pb.Filesystem{},
					Valid:                false,
					CanRead:              false,
					CanWrite:             false,
					ErrorMsg:             "",
					XXX_NoUnkeyedLiteral: struct{}{},
					XXX_unrecognized:     nil,
					XXX_sizecache:        0,
				},
			},
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		},
	}

	for i, sName := range sNames {
		msg := &pb.GetStorageInfo{
			Name: sName,
		}
		resp, err := cl.processStorageInfo(msg)
		if err != nil {
			t.Fatalf("Cannot process storage info for [%d] %q: %s", i, sName, err)
			continue
		}
		if !reflect.DeepEqual(resp, want[i]) {
			t.Errorf("Storage info is not valid for [%d] %s\n%s", i, sName, pretty.Diff(resp, want[i]))
		}
	}
}

func TestRestore(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	bucket := fmt.Sprintf("pbm-test-bucket-%d", 80831)
	tmpDir := os.TempDir()
	minioEndpoint := os.Getenv("MINIO_ENDPOINT")

	st := &storage.Storages{
		Storages: map[string]storage.Storage{
			"s3-us-west": {
				Type: "s3",
				S3: storage.S3{
					Region: "us-west-2",
					Bucket: bucket,
					Credentials: storage.Credentials{
						AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
						SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
					},
				},
				Filesystem: storage.Filesystem{},
			},
			"local-filesystem": {
				Type: "filesystem",
				Filesystem: storage.Filesystem{
					Path: tmpDir,
				},
			},
			"minio": {
				Type: "s3",
				S3: storage.S3{
					Region:      "us-west-2",
					EndpointURL: minioEndpoint,
					Bucket:      bucket,
					Credentials: storage.Credentials{
						AccessKeyID:     os.Getenv("MINIO_ACCESS_KEY_ID"),
						SecretAccessKey: os.Getenv("MINIO_SECRET_ACCESS_KEY"),
					},
				},
				Filesystem: storage.Filesystem{},
			},
		},
	}

	input := InputOptions{
		BackupDir: tmpDir,
		Storages:  st,
		DbConnOptions: ConnectionOptions{
			Host:           testutils.MongoDBHost,
			Port:           testutils.MongoDBShard1PrimaryPort,
			User:           testutils.MongoDBUser,
			Password:       testutils.MongoDBPassword,
			ReplicasetName: testutils.MongoDBShard1ReplsetName,
		},
	}

	dbName := "test"
	colName := "test_col"
	s1Session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		log.Fatalf("Cannot connect to the DB: %s", err)
	}
	s1Session.SetMode(mgo.Strong, true)
	err = s1Session.DB(dbName).C(colName).DropCollection()
	if err != nil {
		t.Errorf("Cannot cleanup database: %s", err)
	}

	cl, err := NewClient(context.Background(), input)
	if err != nil {
		t.Fatalf("Cannot create a new client (node): %s", err)
	}
	if err = cl.dbConnect(); err != nil {
		t.Fatalf("Cannot connect to the db: %s", err)
	}

	msg := &pb.RestoreBackup{
		MongodbHost:       "127.0.0.1:17001",
		BackupType:        pb.BackupType_BACKUP_TYPE_LOGICAL,
		StorageName:       "s3-us-west",
		DbSourceName:      "2019-01-22T17:47:58Z_rs1.dump",
		OplogSourceName:   "2019-01-22T17:47:58Z_rs1.oplog",
		CompressionType:   pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION,
		Cypher:            pb.Cypher_CYPHER_NO_CYPHER,
		OplogStartTime:    0,
		SkipUsersAndRoles: true,
		Host:              "127.0.0.1",
		Port:              "17001",
	}

	err = cl.processRestore(msg)
	if err != nil {
		t.Error(err)
	}
}
