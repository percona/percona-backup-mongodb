package writer

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/globalsign/mgo"
	"github.com/percona/percona-backup-mongodb/internal/awsutils"
	"github.com/percona/percona-backup-mongodb/internal/oplog"
	"github.com/percona/percona-backup-mongodb/internal/storage"
	"github.com/percona/percona-backup-mongodb/internal/testutils"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
	"gopkg.in/mgo.v2/bson"
)

var (
	keepS3Data    bool
	keepLocalData bool
	testStorages  *storage.Storages
)

func TestMain(m *testing.M) {
	flag.BoolVar(&keepS3Data, "keep-s3-data", false, "Do not delete S3 testing bucket and file")
	flag.BoolVar(&keepLocalData, "keep-local-data", false, "Do not files downloaded from the S3 bucket")
	flag.Parse()

	os.Exit(m.Run())
}

func TestWriteToLocalFs(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	filename := "percona-s3-test.oplog"

	mdbSession, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		t.Errorf("Cannot connect to MongoDB: %s", err)
		t.Fail()
	}
	defer mdbSession.Close()

	stopWriter := make(chan bool)
	ticker := time.NewTicker(200 * time.Millisecond)
	go generateOplogTraffic(t, mdbSession, ticker, stopWriter)

	// Start tailing the oplog
	oplogTailer, err := oplog.Open(mdbSession)
	if err != nil {
		t.Fatalf("Cannot instantiate the oplog tailer: %s", err)
	}

	// Run the oplog tailer for a second to collect some documents
	go func() {
		time.Sleep(2 * time.Second)
		oplogTailer.Close()
		stopWriter <- true
	}()

	st := testingStorages()

	bw, err := NewBackupWriter(filename, st.Storages["local-filesystem"], pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION, pb.Cypher_CYPHER_NO_CYPHER)
	if err != nil {
		t.Fatalf("cannot create a new backup writer: %s", err)
	}

	_, err = io.Copy(bw, oplogTailer)
	if err != nil {
		t.Errorf("cannot copy to the s3: %s", err)
	}
	bw.Close()

	oplogFile := filepath.Join(st.Storages["local-filesystem"].Filesystem.Path, filename)
	fi, err := os.Stat(oplogFile)
	if err != nil {
		t.Errorf("Error checking if backup exists: %s", err)
	}
	if fi != nil && fi.Size() == 0 {
		t.Errorf("iInvalid Oplog backup file %s size: %d, %v", oplogFile, fi.Size(), err)
	}
}

func TestUploadToS3(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	filename := "percona-s3-test.oplog"
	st := testingStorages()
	bucket := st.Storages["s3-us-west"].S3.Bucket
	fmt.Printf("Using bucket %q\n", bucket)

	mdbSession, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		t.Errorf("Cannot connect to MongoDB: %s", err)
		t.Fail()
	}
	defer mdbSession.Close()

	stopWriter := make(chan bool)
	ticker := time.NewTicker(200 * time.Millisecond)
	go generateOplogTraffic(t, mdbSession, ticker, stopWriter)

	// Initialize a session in us-west-2 that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.
	sess, err := awsutils.GetAWSSessionFromStorage(st.Storages["s3-us-west"].S3)
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
		time.Sleep(60 * time.Second)
		oplogTailer.Close()
		stopWriter <- true
		fmt.Println("salilendo")
	}()

	bw, err := NewBackupWriter(filename, st.Storages["s3-us-west"], pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION, pb.Cypher_CYPHER_NO_CYPHER)
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

func TestUploadBigFileToS3(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	filename := "percona-s3-test.oplog"
	st := testingStorages()
	bucket := st.Storages["s3-us-west"].S3.Bucket
	fmt.Printf("Using bucket %q\n", bucket)

	mdbSession, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		t.Errorf("Cannot connect to MongoDB: %s", err)
		t.Fail()
	}
	defer mdbSession.Close()

	// Initialize a session in us-west-2 that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.
	sess, err := awsutils.GetAWSSessionFromStorage(st.Storages["s3-us-west"].S3)
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

	bw, err := NewBackupWriter(filename, st.Storages["s3-us-west"], pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION, pb.Cypher_CYPHER_NO_CYPHER)
	if err != nil {
		t.Fatalf("cannot create a new backup writer: %s", err)
	}

	maxSize := int64(6 * 1024 * 1024)
	chunkSize := int64(64 * 1024)
	str := strings.Repeat("a", int(chunkSize))
	size := int64(0)
	for {
		bw.Write([]byte(str))
		size += chunkSize
		if size > maxSize {
			break
		}
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

	fi, err := awsutils.S3Stat(svc, bucket, filename)
	if err != nil {
		t.Errorf("Cannot get stats for file %s in bucket %s", filename, bucket)
	}
	if *fi.Size != size {
		t.Errorf("Invalid file size on S3. Want: %d, have %d", size, *fi.Size)
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

func generateOplogTraffic(t *testing.T, session *mgo.Session, ticker *time.Ticker, stop chan bool) {
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

func testingStorages() *storage.Storages {
	if testStorages != nil {
		return testStorages
	}
	tmpDir := os.TempDir()
	st := &storage.Storages{
		Storages: map[string]storage.Storage{
			"s3-us-west": {
				Type: "s3",
				S3: storage.S3{
					Region: "us-west-2",
					//EndpointURL: "https://minio",
					Bucket: randomBucket(),
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
					Path: filepath.Join(tmpDir, "dump_test"),
				},
			},
		},
	}
	return st
}

func randomBucket() string {
	rand.Seed(time.Now().UnixNano())
	return fmt.Sprintf("pbm-test-bucket-%05d", rand.Int63n(99999))
}
