package oplog

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/percona-backup-mongodb/bsonfile"
	"github.com/percona/percona-backup-mongodb/internal/awsutils"
	"github.com/percona/percona-backup-mongodb/internal/storage"
	"github.com/percona/percona-backup-mongodb/internal/testutils"
	"github.com/percona/percona-backup-mongodb/internal/writer"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
)

const (
	MaxBSONSize = 16 * 1024 * 1024 // 16MB - maximum BSON document size
)

var (
	keepSamples bool
	keepS3Data  bool
	samplesDir  string
	storages    *storage.Storages
)

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

func TestMain(m *testing.M) {
	flag.BoolVar(&keepSamples, "keep-samples", false, "Keep generated bson files")
	flag.BoolVar(&keepS3Data, "keep-s3-data", false, "Keep generated S3 data (buckets and files)")
	flag.Parse()

	// Get root repository path using Git
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		samplesDir = "../../testData"
	} else {
		samplesDir = path.Join(strings.TrimSpace(string(out)), "testdata")
	}

	if testing.Verbose() {
		fmt.Printf("Samples & helper binaries at %q\n", samplesDir)
	}
	os.Exit(m.Run())
}

func TestDetermineOplogCollectionName(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		t.Fatalf("Cannot connect to MongoDB: %s", err)
	}
	defer session.Close()

	oplogCol, err := determineOplogCollectionName(session)
	if err != nil {
		t.Errorf("Cannot determine oplog collection name: %s", err)
	}

	if oplogCol == "" {
		t.Errorf("Cannot determine oplog collection name. Got empty string")
	}
}

func TestBasicReader(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		t.Fatalf("Cannot connect to MongoDB: %s", err)
	}
	defer session.Close()

	stopWriter := make(chan bool)
	go generateOplogTraffic(t, session, stopWriter)

	ot, err := Open(session)
	if err != nil {
		t.Fatalf("Cannot instantiate the oplog tailer: %s", err)
	}
	defer ot.Close()

	buf := make([]byte, MaxBSONSize)
	n, err := ot.Read(buf)
	stopWriter <- true
	if err != nil {
		t.Errorf("Got error reading the oplog: %s", err)
	}
	if n == 0 {
		t.Errorf("Got empty oplog document")
	}

	got := bson.Raw{}
	err = bson.Unmarshal(buf[:n], &got)
	if err != nil {
		t.Errorf("cannot unmarshal: %s", err)
	}

	o := bson.D{}
	err = bson.Unmarshal(got.Data, &o)
	if err != nil {
		t.Errorf("Cannot unmarshal raw data: %s", err)
	}
}

func TestTailerCopy(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "example.bson.")
	if err != nil {
		log.Fatal(err)
	}

	if keepSamples {
		fmt.Printf("Using temp file to dump the oplog using copy: %s\n", tmpfile.Name())
	} else {
		defer os.Remove(tmpfile.Name()) // clean up
	}

	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		t.Fatalf("Cannot connect to MongoDB: %s", err)
	}

	// Start tailing the oplog
	ot, err := Open(session)
	if err != nil {
		t.Fatalf("Cannot instantiate the oplog tailer: %s", err)
	}
	defer session.Close()

	// Let the oplog tailer to run in the background for 1 second to collect
	// some oplog documents
	stopWriter := make(chan bool)
	go generateOplogTraffic(t, session, stopWriter)
	go func() {
		time.Sleep(1 * time.Second)
		ot.Close()
	}()
	stopWriter <- true

	// Use the OplogTail Reader interface to write the documents to a file.
	io.Copy(tmpfile, ot)
	tmpfile.Close()

	err = exec.Command(path.Join(samplesDir, "bsondump"), tmpfile.Name()).Run()
	if err != nil {
		t.Errorf("Error while trying to dump the generated file using bsondump: %s\n", err)
	}
}

func TestSeveralOplogDocTypes(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		t.Fatalf("Failed to connect to primary: %v", err)
	}

	// Start tailing the oplog
	defer session.Close()

	ot, err := Open(session)
	if err != nil {
		t.Fatalf("Cannot instantiate the oplog tailer: %s", err)
	}

	// Let the oplog tailer to run in the background for 1 second to collect
	// some oplog documents
	stopWriter := make(chan bool)
	go generateOplogTraffic(t, session, stopWriter)
	go func() {
		time.Sleep(2 * time.Second)
		ot.Close()
	}()
	stopWriter <- true

	for {
		// Read a document from the Oplog tailer
		buf := make([]byte, MaxBSONSize)
		n, err := ot.Read(buf)
		if err != nil {
			if err != io.EOF {
				t.Errorf("Cannot read a document from the oplog tailer: %s", err)
			}
			break // Stop testing on the first error
		}

		// Unmarshal the document into a bson.D struct. Later, we are going to
		// compare this bson against the one we have stored in file on disk
		originalBson := bson.D{}
		err = bson.Unmarshal(buf[:n], &originalBson)
		if err != nil {
			t.Errorf("Cannot unmarshal the document read from the tailer: %s", err)
			break
		}

		// Create a temporary file on disk.
		tmpfile, err := ioutil.TempFile("", "example.bson.")
		if err != nil {
			log.Fatal(err)
		}
		// Remember to clean up
		if !keepSamples {
			defer os.Remove(tmpfile.Name())
		} else {
			fmt.Printf("Writing bson file: %s\n", tmpfile.Name())
		}
		// Write the oplog document to the file
		tmpfile.Write(buf[:n])
		tmpfile.Close()

		// Read the oplog document from the file on disk.
		readBson := bson.D{}
		bf, err := bsonfile.OpenFile(tmpfile.Name())
		if err != nil {
			t.Errorf("Cannot open bson file %s: %s", tmpfile.Name(), err)
			break
		}
		err = bf.UnmarshalNext(&readBson)
		bf.Close()
		if err != nil {
			t.Errorf("Cannot unmarshal %s contents: %s", tmpfile.Name(), err)
			break
		}

		// Compare the document from the file on disk against the original
		// oplog document from the oplog tailer.
		if !reflect.DeepEqual(readBson, originalBson) {
			t.Errorf("Documents are different")
		}
	}
}

func TestUploadToS3Writer(t *testing.T) {
	storages := testingStorages()
	storageName := "s3-us-west"
	s3Storage, err := storages.Get(storageName)
	if err != nil {
		t.Fatalf("Cannot get s3 storage %s: %s", storageName, err)
	}

	bucket := storages.Storages[storageName].S3.Bucket
	filename := "percona-s3-streamer-test-file"

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
	sess, err := awsutils.GetAWSSessionFromStorage(s3Storage.S3)
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

		// Wait until bucket is created
		fmt.Printf("Waiting for bucket %q to be created...\n", bucket)

		err = svc.WaitUntilBucketExists(&s3.HeadBucketInput{
			Bucket: aws.String(bucket),
		})
		if err != nil {
			t.Errorf("Error while waiting the S3 bucket to be created: %s", err)
			t.Fail()
		}
	}

	// Start tailing the oplog
	oplog, err := Open(mdbSession)
	if err != nil {
		t.Fatalf("Cannot instantiate the oplog tailer: %s", err)
	}

	// Run the oplog tailer for a second to collect some documents
	go func() {
		time.Sleep(4 * time.Second)
		oplog.Close()
		stopWriter <- true
	}()

	// func NewBackupWriter(stg storage.Storage, name string, compressionType pb.CompressionType, cypher pb.Cypher) (*BackupWriter, error) {
	bw, err := writer.NewBackupWriter(s3Storage, filename, pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION, pb.Cypher_CYPHER_NO_CYPHER)
	if err != nil {
		t.Fatalf("cannot create a new backup writer: %s", err)
	}

	_, err = io.Copy(bw, oplog)
	if err != nil {
		t.Errorf("cannot copy to the s3: %s", err)
	}
	bw.Close()

	fileExistsOnS3 := false
	// retry 3 times because ListObjects can be slow to detect new files
	for i := 0; i < 3 && !fileExistsOnS3; i++ {
		resp, err := svc.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(bucket)})
		if err != nil {
			t.Errorf("Cannot list items in the S3 bucket %s: %s", bucket, err)
			t.Fail()
		}

		for _, item := range resp.Contents {
			if *item.Key == filename {
				fileExistsOnS3 = true
				break
			}
		}
		if fileExistsOnS3 {
			break
		}
		time.Sleep(30 * time.Second)
	}
	if !fileExistsOnS3 {
		t.Errorf("File %s doesn't exists on the %s S3 bucket", filename, bucket)
		t.Fail()
	}

	if !keepS3Data {
		if err := awsutils.EmptyBucket(svc, bucket); err != nil {
			t.Errorf("Cannot empty bucket %q: %s", bucket, err)
		}
		_, err = svc.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String(bucket),
		})
		if err != nil {
			t.Errorf("Unable to delete bucket %q, %v", bucket, err)
		}

		// Wait until bucket is deleted before finishing
		fmt.Printf("Waiting for bucket %q to be deleted...\n", bucket)

		err = svc.WaitUntilBucketNotExists(&s3.HeadBucketInput{
			Bucket: aws.String(bucket),
		})

		if err != nil {
			t.Errorf("Error occurred while waiting for bucket to be deleted, %s", bucket)
		}
	}
}

// This is a basic test. S3 uploader only allow to send 5 Mb at a time.
// That's why we are only running the tailer for just 1 second.
// The correct way to send a complete oplog tail is using an S3 streamer.
func TestUploadOplogToS3(t *testing.T) {
	if os.Getenv("DISABLE_AWS_TESTS") == "1" {
		t.Skip("Env var DISABLE_AWS_TESTS=1. Skipping this test")
	}
	bucket := fmt.Sprintf("percona-backup-mongodb-test-%05d", rand.Int63n(100000))
	filename := "percona-backup-mongodb-oplog"

	mdbSession, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		t.Errorf("Cannot connect to MongoDB: %s", err)
		t.Fail()
	}
	defer mdbSession.Close()

	// Initialize a session in us-west-2 that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.
	sess, err := session.NewSession(nil)
	if err != nil {
		t.Skipf("Cannot start AWS session. Skipping S3 test: %s", err)
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

	// Start tailing the oplog
	oplog, err := Open(mdbSession)
	if err != nil {
		t.Fatalf("Cannot instantiate the oplog tailer: %s", err)
	}

	// Run the oplog tailer for a second to collect some documents
	go func() {
		time.Sleep(1 * time.Second)
		oplog.Close()
	}()

	uploader := s3manager.NewUploader(sess)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
		Body:   oplog,
	})
	if err != nil {
		t.Errorf("Cannot upload the oplog to the S3 bucket: %s", err)
		t.Fail()
	}

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

	// Clean up after testing
	_, err = svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(filename)})
	if err != nil {
		t.Errorf("Unable to delete object %q from bucket %q, %v", filename, bucket, err)
	}

	err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
	})
	if err != nil {
		t.Errorf("File %s was not deleted from the %s bucket: %s", filename, bucket, err)
	}

	_, err = svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Errorf("Unable to delete bucket %q, %v", bucket, err)
	}

	// Wait until bucket is deleted before finishing
	fmt.Printf("Waiting for bucket %q to be deleted...\n", bucket)

	err = svc.WaitUntilBucketNotExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Errorf("Error occurred while waiting for bucket to be deleted, %s", bucket)
	}
}

func TestReadIntoSmallBuffer(t *testing.T) {
	ot := &OplogTail{
		dataChan:       make(chan chanDataTye, 1),
		stopChan:       make(chan bool),
		readerStopChan: make(chan bool),
		running:        true,
	}
	ot.readFunc = makeReader(ot)

	testString := "0123456789ABC"
	// simulate a read from the tailer by putting some data into the data channel
	ot.dataChan <- []byte(testString)

	// The for loop below will end on EOF. I could have written:
	// for i:=0; i < 4; i++ (since the testString has 13 bytes but I am reading in
	// chunks of 4 bytes) and then send the stop signal after the for loop ends, but
	// the idea is to also test the reader sends the EOF error correctly.
	go func() {
		time.Sleep(100 * time.Millisecond) // dataChan is buffered so <- is not immediate
		close(ot.readerStopChan)
	}()

	result := []byte{}
	totalSize := 0
	for {
		buf := make([]byte, 4)
		n, err := ot.Read(buf)
		if err != nil {
			break
		}
		totalSize += n
		result = append(result, buf[:n]...)
	}

	if totalSize != len(testString) {
		t.Errorf("Received data has a wrong size. Want %d, got %d", len(testString), totalSize)
	}
	if string(result) != testString {
		t.Errorf("Invalid received data. Want: %q, got %q", testString, string(result))
	}
}

func testingStorages() *storage.Storages {
	if storages != nil {
		return storages
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
	storages = st
	return st
}

func randomBucket() string {
	rand.Seed(time.Now().UnixNano())
	return fmt.Sprintf("pbm-test-bucket-%05d", rand.Int63n(99999))
}
