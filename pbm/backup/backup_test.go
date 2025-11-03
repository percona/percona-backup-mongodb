package backup

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	stds3 "github.com/percona/percona-backup-mongodb/pbm/storage/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
)

func TestMetadataEncodeDecodeWithMinio(t *testing.T) {
	ctx := context.Background()

	minioContainer, err := minio.Run(ctx, "minio/minio:RELEASE.2024-08-17T01-24-54Z")
	defer func() {
		if err := testcontainers.TerminateContainer(minioContainer); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	if err != nil {
		t.Fatalf("failed to start container: %s", err)
	}

	endpoint, err := minioContainer.Endpoint(ctx, "http")
	if err != nil {
		t.Fatalf("failed to get endpoint: %s", err)
	}

	defaultConfig, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithBaseEndpoint(endpoint),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", ""),
		),
	)
	if err != nil {
		t.Fatalf("failed to load config: %s", err)
	}

	s3Client := s3.NewFromConfig(defaultConfig, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	bucketName := "test-bucket"
	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	if err != nil {
		t.Errorf("failed to create bucket: %s", err)
	}

	opts := &stds3.Config{
		EndpointURL: endpoint,
		Bucket:      bucketName,
		Credentials: stds3.Credentials{
			AccessKeyID:     "minioadmin",
			SecretAccessKey: "minioadmin",
		},
		Retryer:               &stds3.Retryer{},
		ServerSideEncryption:  &stds3.AWSsse{},
		InsecureSkipTLSVerify: true,
	}

	stg, err := stds3.New(opts, "node", nil)
	if err != nil {
		t.Fatalf("failed to create s3 storage: %s", err)
	}

	for i := range 10 {
		t.Logf("decode->save->read->encode try #%d", i+1)
		wMeta := getMetaDoc(t)

		err = writeMeta(stg, wMeta)
		if err != nil {
			t.Fatalf("dump metadata: %v", err)
		}

		rMeta, err := ReadMetadata(stg, wMeta.Name+defs.MetadataFileSuffix)
		if err != nil {
			t.Fatalf("read metadata: %v", err)
		}
		diff := cmp.Diff(wMeta, rMeta, cmpopts.IgnoreFields(BackupMeta{}, "runtimeError"))
		if diff != "" {
			t.Fatalf("meta is different: %v", diff)
		}
	}
}

func getMetaDoc(t *testing.T) *BackupMeta {
	t.Helper()

	metaJson, err := os.ReadFile(filepath.Join("./testdata", "metaDoc.json"))
	if err != nil {
		t.Fatalf("failed to read test json file:%v", err)
	}

	doc := &BackupMeta{}
	err = json.Unmarshal(metaJson, doc)
	if err != nil {
		t.Fatal("unmarshal test doc ", err)
	}

	return doc
}
