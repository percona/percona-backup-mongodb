package writer

import (
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/globalsign/mgo"
	"github.com/percona/percona-backup-mongodb/internal/awsutils"
	"github.com/percona/percona-backup-mongodb/internal/oplog"
	"github.com/percona/percona-backup-mongodb/internal/testutils"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
	"gopkg.in/mgo.v2/bson"
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

func TestWriteToLocalFs(t *testing.T) {
	filename := "percona-s3-test.oplog"

	mdbSession, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		t.Errorf("Cannot connect to MongoDB: %s", err)
		t.Fail()
	}
	defer mdbSession.Close()

	stopWriter := make(chan bool)
	go generateOplogTraffic(t, mdbSession, stopWriter)

	// Start tailing the oplog
	oplogTailer, err := oplog.Open(mdbSession)
	if err != nil {
		t.Fatalf("Cannot instantiate the oplog tailer: %s", err)
	}

	// Run the oplog tailer for a second to collect some documents
	go func() {
		time.Sleep(12 * time.Second)
		oplogTailer.Close()
		stopWriter <- true
	}()

	storages := testutils.TestingStorages()
	storageName := "local-filesystem"
	localStg, err := storages.Get(storageName)
	if err != nil {
		t.Fatalf("Cannot get storage %q: %s", storageName, err)
	}
	err = os.MkdirAll(localStg.Filesystem.Path, os.ModePerm)
	defer os.RemoveAll(localStg.Filesystem.Path)

	bw, err := NewBackupWriter(localStg, filename, pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION, pb.Cypher_CYPHER_NO_CYPHER)
	if err != nil {
		t.Fatalf("cannot create a new backup writer: %s", err)
	}

	_, err = io.Copy(bw, oplogTailer)
	if err != nil {
		t.Errorf("cannot copy to the s3: %s", err)
	}
	bw.Close()

	oplogFile := filepath.Join(localStg.Filesystem.Path, filename)
	fi, err := os.Stat(oplogFile)
	if err != nil {
		t.Errorf("Error checking if backup exists: %s", err)
	}
	if fi != nil && fi.Size() == 0 {
		t.Errorf("iInvalid Oplog backup file %s size: %d, %v", oplogFile, fi.Size(), err)
	}
}

func TestUploadToS3(t *testing.T) {
	storages := testutils.TestingStorages()
	storageName := "s3-us-west"
	s3Stg, err := storages.Get(storageName)
	if err != nil {
		t.Fatalf("Cannot get storage %q: %s", storageName, err)
	}

	bucket := s3Stg.S3.Bucket
	filename := "percona-s3-test.oplog"

	mdbSession, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		t.Errorf("Cannot connect to MongoDB: %s", err)
		t.Fail()
	}
	defer mdbSession.Close()

	stopWriter := make(chan bool)
	go generateOplogTraffic(t, mdbSession, stopWriter)

	// Initialize a session in us-west-2 that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.
	sess, err := awsutils.GetAWSSessionFromStorage(s3Stg.S3)
	if err != nil {
		t.Fatalf("Cannot start AWS session. Skipping S3 test: %s", err)
	}

	// Create S3 service client
	svc := s3.New(sess)

	exists, err := awsutils.BucketExists(svc, bucket)
	if err != nil {
		t.Fatalf("Cannot check if bucket exists %s: %s", bucket, err)
	}
	if !exists {
		_, err = svc.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(bucket),
		})
		if err != nil {
			t.Errorf("Unable to create bucket %q, %v", bucket, err)
			t.Fail()
		}

		err = svc.WaitUntilBucketExists(&s3.HeadBucketInput{
			Bucket: aws.String(bucket),
		})
		if err != nil {
			t.Errorf("Error while waiting the S3 bucket to be created: %s", err)
			t.Fail()
		}
	}

	if err := awsutils.EmptyBucket(svc, bucket); err != nil {
		t.Fatalf("Cannot empty bucket %q: %s", bucket, err)
	}
	// Start tailing the oplog
	oplogTailer, err := oplog.Open(mdbSession)
	if err != nil {
		t.Fatalf("Cannot instantiate the oplog tailer: %s", err)
	}

	// Run the oplog tailer for a second to collect some documents
	go func() {
		time.Sleep(4 * time.Second)
		oplogTailer.Close()
		stopWriter <- true
	}()

	bw, err := NewBackupWriter(s3Stg, filename, pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION, pb.Cypher_CYPHER_NO_CYPHER)
	if err != nil {
		t.Fatalf("cannot create a new backup writer: %s", err)
	}

	_, err = io.Copy(bw, oplogTailer)
	if err != nil {
		t.Errorf("cannot copy to the s3: %s", err)
	}
	bw.Close()

	time.Sleep(3 * time.Second)
	// Check the file was really uploaded
	resp, err := svc.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Errorf("Cannot list items in the S3 bucket %s: %s", bucket, err)
		t.Fail()
	}

	fileExistsOnS3 := false
	for _, item := range resp.Contents {
		if *item.Key == filename {
			fileExistsOnS3 = true
			break
		}
	}
	if !fileExistsOnS3 {
		t.Errorf("File %s doesn't exists on the %s S3 bucket", filename, bucket)
		t.Fail()
	}

	if !keepS3Data {
		if err := awsutils.EmptyBucket(svc, bucket); err != nil {
			t.Fatalf("Cannot empty bucket %q: %s", bucket, err)
		}
		_, err = svc.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String(bucket),
		})
		if err != nil {
			t.Errorf("Unable to delete bucket %q, %v", bucket, err)
		}

		err = svc.WaitUntilBucketNotExists(&s3.HeadBucketInput{
			Bucket: aws.String(bucket),
		})
		if err != nil {
			t.Errorf("Error occurred while waiting for bucket to be deleted, %s", bucket)
		}
	}
}

func generateOplogTraffic(t *testing.T, session *mgo.Session, stop chan bool) {
	ticker := time.NewTicker(200 * time.Millisecond)
	for {
		select {
		case <-stop:
			ticker.Stop()
			return
		case <-ticker.C:
			err := session.DB("test").C("test").Insert(bson.M{"t": t.Name()})
			if err != nil {
				t.Logf("insert to test.test failed: %v", err.Error())
			}
		}
	}
}
