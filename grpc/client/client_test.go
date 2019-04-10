package client

import (
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/kr/pretty"
	"github.com/percona/percona-backup-mongodb/internal/reader"
	"github.com/percona/percona-backup-mongodb/internal/testutils"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
	log "github.com/sirupsen/logrus"
)

const (
	dbName = "test001"
	col1   = "col1"
	col2   = "col2"
)

func TestMain(m *testing.M) {
	exitStatus := m.Run()
	if os.Getenv("KEEP_DATA") == "" {
		if err := testutils.CleanTempDirAndBucket(); err != nil {
			fmt.Printf("Cannot clean up directory and bucket: %s", err)
		}
	} else {
		fmt.Println("Keeping testing data (env var KEEP_DATA is not empty)")
		fmt.Printf("Directory %s was not deleted\n", testutils.TestingStorages().Storages["local-filesystem"].Filesystem.Path)
		fmt.Printf("S3 bucket %s was not deleted\n", testutils.TestingStorages().Storages["s3-us-west"].S3.Bucket)
	}
	os.Exit(exitStatus)
}

func TestValidateFilesystemStorage(t *testing.T) {
	input, err := buildInputParams()
	if err != nil {
		t.Fatalf("Cannot build agent's input params: %s", err)
	}
	c, err := NewClient(context.TODO(), input)
	if err != nil {
		t.Fatalf("Cannot create a new client: %s", err)
	}

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
	if err != nil {
		t.Fatalf("Cannot create a client: %s", err)
	}

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

func TestFsBackupAndRestore(t *testing.T) {
	testBackupAndRestore(t, "local-filesystem")
}

func TestMinioBackupAndRestore(t *testing.T) {
	testBackupAndRestore(t, "minio")
}

func TestS3sBackupAndRestore(t *testing.T) {
	testBackupAndRestore(t, "s3-us-west")
}

func testBackupAndRestore(t *testing.T, storage string) {
	ndocs := 100000
	bulkSize := 5000

	input, err := buildInputParams()
	if err != nil {
		t.Fatalf("Cannot build agent's input params: %s", err)
	}

	c, err := NewClient(context.TODO(), input)
	if err != nil {
		t.Fatalf("Cannot get S3 storage: %s", err)
	}
	if err := c.dbConnect(); err != nil {
		t.Fatalf("Cannot connect to the db: %s", err)
	}

	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		log.Fatalf("Cannot connect to the DB: %s", err)
	}

	session.SetMode(mgo.Strong, true)

	generateDataToBackup(t, c.mdbSession, dbName, col1, ndocs, bulkSize)
	generateDataToBackup(t, c.mdbSession, dbName, col2, ndocs, bulkSize)
	defer dropCollections(t, c.mdbSession, dbName, col1, col2)

	msg := &pb.StartBackup{
		BackupType:      pb.BackupType_BACKUP_TYPE_LOGICAL,
		DbBackupName:    "0001.dump",
		OplogBackupName: "",
		CompressionType: pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION,
		Cypher:          pb.Cypher_CYPHER_NO_CYPHER,
		OplogStartTime:  0,
		Description:     "test001",
		StorageName:     storage,
	}

	err = c.runDBBackup(msg)
	if err != nil {
		t.Errorf("Cannot process restore from s3: %s", err)
	}

	rmsg := &pb.RestoreBackup{
		MongodbHost: "127.0.0.1",
		BackupType:  pb.BackupType_BACKUP_TYPE_LOGICAL,
		//SourceBucket
		DbSourceName:      "0001.dump",
		OplogSourceName:   "",
		CompressionType:   pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION,
		Cypher:            pb.Cypher_CYPHER_NO_CYPHER,
		OplogStartTime:    0,
		SkipUsersAndRoles: true,
		Host:              "127.0.0.1",
		Port:              "17001",
		StorageName:       storage,
	}

	err = c.restoreDBDump(rmsg)
	if err != nil {
		t.Errorf("Cannot process restore from s3: %s", err)
	}

}

func TestWriteFile(t *testing.T) {
	input, err := buildInputParams()
	if err != nil {
		t.Fatalf("Cannot build agent's input params: %s", err)
	}

	c, err := NewClient(context.TODO(), input)
	if err != nil {
		t.Fatalf("Cannot instantiate a new client: %s", err)
	}

	tests := map[string]struct {
		storageName string
		fileName    string
		wantErr     bool
	}{
		"valid-local":      {storageName: "local-filesystem", fileName: "file-001.txt", wantErr: false},
		"invalid-storage":  {storageName: "invalid-storage", fileName: "file-001.txt", wantErr: true},
		"invalid-filename": {storageName: "local-filesystem", fileName: "/non-existent-dir/file-001.txt", wantErr: true},
		"s3":               {storageName: "s3-us-west", fileName: "file-001.txt", wantErr: false},
	}
	for name, args := range tests {
		testName := name
		fn := args.fileName
		sn := args.storageName
		wantErr := args.wantErr
		t.Run(name, func(t *testing.T) {
			txt := []byte("I am the man with no name. Zapp Brannigan at your service.")
			msg := &pb.WriteFile{
				StorageName: sn,
				FileName:    fn,
				Data:        txt,
			}

			resp, err := c.processWriteFile(msg)
			if wantErr {
				if err == nil {
					t.Errorf("Error expected in test %s, got nil instead", testName)
				}
				return
			}
			if err != nil {
				t.Errorf("Cannot write file %s: %s", msg.FileName, err)
			}
			if resp.GetWriteStatus().Error != "" {
				t.Errorf("Write file (%s at %s) returned an error: %s", msg.FileName, msg.StorageName, err)
			}

			stg, err := testutils.TestingStorages().Get(msg.StorageName)
			if err != nil {
				t.Errorf("Cannot get storage %s: %s", msg.StorageName, err)
			}
			br, err := reader.MakeReader(msg.FileName,
				stg,
				pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION,
				pb.Cypher_CYPHER_NO_CYPHER,
			)
			if err != nil {
				t.Errorf("Cannot create a reader: %s", err)
			}
			b := make([]byte, len(txt))
			n, err := br.Read(b)
			if err != nil && err != io.EOF {
				t.Errorf("Cannot read %s file from %s storage: %s", msg.FileName, msg.StorageName, err)
			}
			if n != len(txt) {
				t.Errorf("Invalid read bytes count. Got %d, want %d.\n%s\n%s", n, len(txt), string(txt), string(b))
			}
			if err := br.Close(); err != nil {
				t.Errorf("Cannot close file: %s", err)
			}
			if string(txt) != string(b) {
				t.Errorf("file content is invalid. Got:\n%s\nWant:\n%s", string(b), string(txt))
			}
		})
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
		Host:           testutils.MongoDBHost,
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

func generateDataToBackup(t *testing.T, session *mgo.Session, dbName string, colName string, ndocs, bulkSize int) {
	// Don't check for error because the collection might not exist.
	dropCollections(t, session, dbName, colName)
	if err := session.DB(dbName).C(colName).EnsureIndexKey("number"); err != nil {
		t.Logf("Cannot ensure index 'number' in collection %s.%s: %s", dbName, colName, err)
	}

	number := 0
	for i := 0; i < ndocs/bulkSize; i++ {
		docs := make([]interface{}, 0, bulkSize)
		bulk := session.DB(dbName).C(colName).Bulk()
		for j := 0; j < bulkSize; j++ {
			number++
			docs = append(docs, bson.M{"number": number})
		}
		bulk.Insert(docs...)
		_, err := bulk.Run()
		if err != nil {
			t.Fatalf("Cannot insert data to back up: %s", err)
		}
	}
}

func dropCollections(t *testing.T, session *mgo.Session, dbName string, cols ...string) {
	colsInDB, err := session.DB(dbName).CollectionNames()
	if err != nil {
		t.Errorf("Cannot get the collections list in %q db", dbName)
		return
	}
	for _, colToDelete := range cols {
		for _, col := range colsInDB {
			if col == colToDelete {
				if err := session.DB(dbName).C(colToDelete).DropCollection(); err != nil {
					t.Logf("Cannot drop collection %s.%s: %s", dbName, colToDelete, err)
				}
			}
		}
	}
	session.Refresh()
}
