package s3writer

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
)

const (
	megabyte = 1024 * 1024
)

type mockReader struct {
	testString    []byte
	position      int
	previousChunk string
}

type testChunk struct {
	size  int
	value byte
}

var (
	testFileSize int
	fileNotFound = fmt.Errorf("File not found")
	chunks       = []testChunk{
		testChunk{size: s3MinChunkSize, value: '1'},
		testChunk{size: s3MinChunkSize, value: '2'},
		testChunk{size: s3MinChunkSize / 2, value: '3'},
	}
	bucket   = "percona-mongodb-backup-test-s3-streamer"
	filename = "percona-s3-streamer-test-file"
	// Command line parameters for debugging
	keepS3Data     bool
	keepLocalFiles bool
)

func openMockReader() *mockReader {
	m := &mockReader{
		testString: make([]byte, 0, testFileSize),
	}

	for _, chunk := range chunks {
		for i := 0; i < chunk.size; i++ {
			m.testString = append(m.testString, chunk.value)
		}
	}

	return m
}

func TestMain(m *testing.M) {
	flag.BoolVar(&keepS3Data, "keep-s3-data", false, "Do not delete S3 testing bucket and file")
	flag.BoolVar(&keepLocalFiles, "keep-local-files", false, "Do not files downloaded from the S3 bucket")
	flag.Parse()

	testFileSize = 0
	for _, chunk := range chunks {
		testFileSize += chunk.size
	}

	// Randomize to avoid name collisions
	randPart := rand.Int63n(100000)
	bucket = fmt.Sprintf("%s-%05d", bucket, randPart)
	filename = fmt.Sprintf("%s-%05d", filename, randPart)

	if b := os.Getenv("TEST_S3_BUCKET"); b != "" {
		bucket = b
	}

	if f := os.Getenv("TEST_S3_FILE"); f != "" {
		filename = f
	}

	os.Exit(m.Run())
}

func TestMockReader(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "")
	if err != nil {
		t.Errorf("Cannot create temporary file for download: %s", err)
	}
	mr := openMockReader()
	io.Copy(tmpfile, mr)
	mr.Reset()
	tmpfile.Close()

	if err := checkDownloadedFile(tmpfile.Name()); err != nil {
		t.Errorf("Error checking downloading file contents: %s", err)
	}

}

func (m *mockReader) Read(buf []byte) (int, error) {
	if m.position >= len(m.testString) {
		return 0, io.EOF
	}
	respSize := len(buf)
	if m.position+respSize > len(m.testString) {
		respSize = len(m.testString) - m.position
	}
	copy(buf, m.testString[m.position:])
	m.position += respSize
	return respSize, nil
}
func (m *mockReader) Reset() {
	m.position = 0
}

func TestUploadOplogToS3(t *testing.T) {

	// Initialize a session in us-west-2 that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.
	diag("Staring AWS session")
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-2")},
	)
	if err != nil {
		t.Skipf("Cannot start AWS session. Skipping S3 test: %s", err)
	}

	diag("Creating S3 service client")
	// Create S3 service client
	svc := s3.New(sess)
	diag("Checking if bucket %s exists", bucket)
	if _, err := bucketExists(svc, bucket); err != nil {
		t.Error(err)
	}

	exists, err := bucketExists(svc, bucket)
	if err != nil {
		t.Fatalf("Cannot check if bucket %q exists: %s", bucket, err)
	}
	if !exists {
		if err := createBucket(svc, bucket); err != nil {
			t.Fatalf("Unable to create bucket %q, %v", bucket, err)
		}
	}

	diag("Streaming data from mock reader to S3")
	mr := openMockReader()

	s3w, err := Open(sess, bucket, filename)
	if err != nil {
		t.Errorf("Cannot open the s3 writer: %s", err)
		t.Fail()
	}

	n, err := io.Copy(s3w, mr)
	err = s3w.Close()
	if err != nil {
		t.Errorf("Cannot close S3 streamer: %s", err)
	}
	if n != int64(testFileSize) {
		t.Errorf("Uploaded file size is wrong. Want %d, got %d", testFileSize, n)
	}
	diag("File upload completed")

	// Check the file was really uploaded
	diag("Checking file on the S3 bucket")
	fileInfo, err := s3Stat(svc, bucket, filename)
	if err != nil {
		if err == fileNotFound {
			t.Errorf("File %q doesn't exists on the %q bucket", filename, bucket)
		} else {
			t.Errorf("Cannot check if the file %q exists on the %q bucket", filename, bucket)
		}
	}

	if *fileInfo.Size != int64(testFileSize) {
		t.Errorf("File size is wrong. Want %d, got %d", testFileSize, fileInfo.Size)
	}

	diag("Downloading the file to check its content")
	tmpfile, err := ioutil.TempFile("", "")
	if err != nil {
		t.Errorf("Cannot create temporary file for download: %s", err)
	} else {
		downloadFile(svc, bucket, filename, tmpfile)
		tmpfile.Close()
		if err := checkDownloadedFile(tmpfile.Name()); err != nil {
			t.Errorf("Error checking downloading file contents: %s", err)
		}
		if !keepLocalFiles {
			os.Remove(tmpfile.Name())
		} else {
			diag("Removing file downloaded from S3: %s", tmpfile.Name())
		}
	}

	// Clean up after testing
	if !keepS3Data {
		diag("Deleting file %q in %q bucket", filename, bucket)
		if err = deleteFile(svc, bucket, filename); err != nil {
			t.Errorf("Cannot delete file %q from bucket %q: %s", filename, bucket, err)
		}

		diag("Deleting bucket %q", bucket)
		if err = deleteBucket(svc, bucket); err != nil {
			t.Errorf("Cannot delete bucket %q", bucket)
		}
	} else {
		diag("Skipping deletion of %q bucket", bucket)
		diag("Skipping deletion of %q file in %q bucket", filename, bucket)
	}
}

// -----------------//
// Helper functions //
// -----------------//
func bucketExists(svc *s3.S3, bucketname string) (bool, error) {
	input := &s3.ListBucketsInput{}

	result, err := svc.ListBuckets(input)
	if err != nil {
		return false, err
	}
	for _, bucket := range result.Buckets {
		if *bucket.Name == bucketname {
			return true, nil
		}
	}
	return false, nil
}

func createBucket(svc *s3.S3, bucket string) error {
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return errors.Wrapf(err, "Unable to create bucket %q", bucket)
	}

	err = svc.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return errors.Wrap(err, ("error while waiting the S3 bucket to be created"))
	}
	return nil
}

func s3Stat(svc *s3.S3, bucket, filename string) (*s3.Object, error) {
	resp, err := svc.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(bucket)})
	if err != nil {
		return nil, err
	}

	for _, item := range resp.Contents {
		if *item.Key == filename {
			return item, nil
		}
	}
	return nil, fileNotFound
}

func deleteFile(svc *s3.S3, bucket, filename string) error {
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(filename)})
	if err != nil {
		return errors.Wrapf(err, "unable to delete object %q from bucket %q", filename, bucket)
	}

	err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
	})
	if err != nil {
		return errors.Wrapf(err, "file %s was not deleted from the %s bucket", filename, bucket)
	}
	return nil
}

func deleteBucket(svc *s3.S3, bucket string) error {
	_, err := svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return errors.Wrapf(err, "unable to delete bucket %q", bucket)
	}

	err = svc.WaitUntilBucketNotExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return errors.Wrapf(err, "error occurred while waiting for bucket to be deleted, %s", bucket)
	}
	return nil
}

func downloadFile(svc *s3.S3, bucket, file string, writer io.WriterAt) (int64, error) {
	downloader := s3manager.NewDownloaderWithClient(svc)

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(file),
	}

	return downloader.Download(writer, input)
}

func checkDownloadedFile(filename string) error {
	fh, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fh.Close()

	part := 0
	for _, chunk := range chunks {
		part++
		buffer := make([]byte, chunk.size)
		n, err := fh.Read(buffer)
		if err != nil {
			return err
		}
		if n != chunk.size {
			return fmt.Errorf("Chunk #%d size is incorrect. Want %d bytes, got %d bytes", part, n, chunk.size)
		}
		for i := 0; i < len(buffer); i++ {
			if buffer[i] != chunk.value {
				return fmt.Errorf("Chunk #%d content is incorrect. Want %d at position %d, got %v",
					part, chunk.value, i, buffer[i])
			}
		}
	}

	return nil
}

func diag(params ...interface{}) {
	if testing.Verbose() {
		log.Printf(params[0].(string), params[1:]...)
	}
}
