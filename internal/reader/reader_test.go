package reader

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/percona/percona-backup-mongodb/bsonfile"
	"github.com/percona/percona-backup-mongodb/internal/awsutils"
	"github.com/percona/percona-backup-mongodb/internal/storage"
	"github.com/percona/percona-backup-mongodb/internal/writer"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
	"gopkg.in/mgo.v2/bson"
)

func TestFSReader(t *testing.T) {
	dumpFile := "test.dump"

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
					Path: "testdata",
				},
			},
		},
	}

	rdr, err := MakeReader(dumpFile, st.Storages["local-filesystem"], pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION, pb.Cypher_CYPHER_NO_CYPHER)
	if err != nil {
		t.Fatalf("Cannot create a local filesystem reader: %s", err)
	}

	bsonReader, err := bsonfile.NewBSONReader(rdr)
	if err != nil {
		t.Errorf("Cannot create a bson reader %s", err)
	}

	count := 0
	want := 3
	for {
		var doc bson.M
		err := bsonReader.UnmarshalNext(&doc)
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("Error while reading doc from the reader: %s", err)
		}
		count++
	}
	if count != want {
		t.Errorf("Invalid documents count from the reader. Want %d, got %d", want, count)
	}
}

func TestReaderFromS3(t *testing.T) {
	dumpFile := "test.dump"

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
					Path: "testdata",
				},
			},
		},
	}

	sess, err := awsutils.GetAWSSessionFromStorage(st.Storages["s3-us-west"].S3)
	if err != nil {
		t.Fatalf("Cannot get an AWS session: %s", err)
	}
	svc := s3.New(sess)

	fmt.Printf("Creating bucket: %s\n", st.Storages["s3-us-west"].S3.Bucket)
	if err := awsutils.CreateBucket(svc, st.Storages["s3-us-west"].S3.Bucket); err != nil {
		t.Fatalf("Cannot create an S3 buscket %q: %s", st.Storages["s3-us-west"].S3.Bucket, err)
	}
	fmt.Printf("Created bucket: %s\n", st.Storages["s3-us-west"].S3.Bucket)

	defer awsutils.DeleteBucket(svc, st.Storages["s3-us-west"].S3.Bucket)
	defer awsutils.EmptyBucket(svc, st.Storages["s3-us-west"].S3.Bucket)

	fh, err := os.Open(filepath.Join("testdata", dumpFile))
	if err != nil {
		t.Fatalf("Cannot open dump file to upload: %s", err)
	}
	defer fh.Close()

	fmt.Println("Uploading file to the bucket")
	if err := awsutils.UploadFileToS3(sess, fh, st.Storages["s3-us-west"].S3.Bucket, dumpFile); err != nil {
		t.Fatalf("Cannot upload file to S3: %s", err)
	}
	fmt.Println("Uploaded file to the bucket")

	rdr, err := MakeReader(dumpFile, st.Storages["s3-us-west"], pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION, pb.Cypher_CYPHER_NO_CYPHER)
	if err != nil {
		t.Fatalf("Cannot create a local filesystem reader: %s", err)
	}

	bsonReader, err := bsonfile.NewBSONReader(rdr)
	if err != nil {
		t.Fatalf("Cannot create a bson reader %s", err)
	}
	count := 0
	want := 3
	for {
		var doc bson.M
		err := bsonReader.UnmarshalNext(&doc)
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("Error while reading doc from the reader: %s", err)
		}
		count++
	}
	if count != want {
		t.Errorf("Invalid documents count from the reader. Want %d, got %d", want, count)
	}
}

func TestDownloadBigFile(t *testing.T) {
	dumpFile := "test.dump"

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
					Path: "testdata",
				},
			},
		},
	}

	sess, err := awsutils.GetAWSSessionFromStorage(st.Storages["s3-us-west"].S3)
	if err != nil {
		t.Fatalf("Cannot get an AWS session: %s", err)
	}
	svc := s3.New(sess)
	bucket := st.Storages["s3-us-west"].S3.Bucket

	fmt.Printf("Creating bucket: %s\n", st.Storages["s3-us-west"].S3.Bucket)
	if err := awsutils.CreateBucket(svc, st.Storages["s3-us-west"].S3.Bucket); err != nil {
		t.Fatalf("Cannot create an S3 buscket %q: %s", st.Storages["s3-us-west"].S3.Bucket, err)
	}
	fmt.Printf("Created bucket: %s\n", st.Storages["s3-us-west"].S3.Bucket)

	defer awsutils.DeleteBucket(svc, st.Storages["s3-us-west"].S3.Bucket)
	defer awsutils.EmptyBucket(svc, st.Storages["s3-us-west"].S3.Bucket)

	bw, err := writer.NewBackupWriter(dumpFile, st.Storages["s3-us-west"], pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION, pb.Cypher_CYPHER_NO_CYPHER)
	if err != nil {
		t.Fatalf("cannot create a new backup writer: %s", err)
	}

	fmt.Printf("uploading file %s to bucket %s\n", dumpFile, bucket)

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

	fmt.Println("reading")
	rdr, err := MakeReader(dumpFile, st.Storages["s3-us-west"], pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION, pb.Cypher_CYPHER_NO_CYPHER)
	if err != nil {
		t.Fatalf("Cannot create a local filesystem reader: %s", err)
	}

	tmpFile := filepath.Join(os.TempDir(), "dump.test")
	fh, err := os.Create(tmpFile)
	if err != nil {
		t.Fatalf("Cannot create tmp file: %s", err)
	}
	defer os.Remove(tmpFile)

	io.Copy(fh, rdr)
	fh.Close()
	rdr.Close()
	fi, err := os.Stat(tmpFile)
	if err != nil {
		t.Errorf("Cannot stat file %s: %s", tmpFile, err)
	}
	if fi.Size() != size {
		t.Errorf("invalid file %s size. Want %d, got %d", tmpFile, size, fi.Size())
	}
}

func TestDownloadBigDumpFile(t *testing.T) {
	dumpFile := "2019-01-22T17:47:58Z_rs1.dump"
	bucket := fmt.Sprintf("pbm-test-bucket-%d", 80831)

	st := &storage.Storages{
		Storages: map[string]storage.Storage{
			"s3-us-west": {
				Type: "s3",
				S3: storage.S3{
					Region: "us-west-2",
					//EndpointURL: "https://minio",
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
					Path: "testdata",
				},
			},
		},
	}

	rdr, err := MakeReader(dumpFile, st.Storages["s3-us-west"], pb.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION, pb.Cypher_CYPHER_NO_CYPHER)
	if err != nil {
		t.Fatalf("Cannot create a local filesystem reader: %s", err)
	}

	tmpFile := filepath.Join(os.TempDir(), "dump.test")
	fh, err := os.Create(tmpFile)
	if err != nil {
		t.Fatalf("Cannot create tmp file: %s", err)
	}
	//defer os.Remove(tmpFile)

	io.Copy(fh, rdr)
	fh.Close()
	rdr.Close()
}

func randomBucket() string {
	rand.Seed(time.Now().UnixNano())
	return fmt.Sprintf("pbm-test-bucket-%05d", rand.Int63n(99999))
}
