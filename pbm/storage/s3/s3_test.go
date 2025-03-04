package s3

import (
	"context"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

func TestS3(t *testing.T) {
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

	opts := &Config{
		EndpointURL: endpoint,
		Bucket:      bucketName,
		Credentials: Credentials{
			AccessKeyID:     "minioadmin",
			SecretAccessKey: "minioadmin",
		},
		Retryer: &Retryer{},
	}

	stg, err := New(opts, "node", nil)
	if err != nil {
		t.Fatalf("failed to create s3 storage: %s", err)
	}

	storage.RunStorageTests(t, stg, storage.S3)
}

func TestConfig(t *testing.T) {
	opts := &Config{
		Bucket: "bucketName",
		Prefix: "prefix",
		Credentials: Credentials{
			AccessKeyID:     "minioadmin",
			SecretAccessKey: "minioadmin",
		},
		Retryer: &Retryer{
			NumMaxRetries: 1,
		},
		ServerSideEncryption: &AWSsse{},
	}

	t.Run("Clone", func(t *testing.T) {
		opts.ForcePathStyle = aws.Bool(true)
		clone := opts.Clone()
		if clone == opts {
			t.Error("expected clone to be a different pointer")
		}

		if !opts.Equal(clone) {
			t.Error("expected clone to be equal")
		}

		opts.Bucket = "updatedName"
		if opts.Equal(clone) {
			t.Error("expected clone to be unchanged when updating original")
		}
	})

	t.Run("Equal fails", func(t *testing.T) {
		if opts.Equal(nil) {
			t.Error("expected not to be equal other nil")
		}

		clone := opts.Clone()
		clone.Prefix = "updatedPrefix"
		if opts.Equal(clone) {
			t.Error("expected not to be equal when updating prefix")
		}

		clone = opts.Clone()
		clone.Region = "updatedRegion"
		if opts.Equal(clone) {
			t.Error("expected not to be equal when updating region")
		}

		clone = opts.Clone()
		clone.EndpointURL = "EndpointURL"
		if opts.Equal(clone) {
			t.Error("expected not to be equal when updating EndpointURL")
		}

		clone = opts.Clone()
		clone.EndpointURLMap = map[string]string{"foo": "bar"}
		if opts.Equal(clone) {
			t.Error("expected not to be equal when updating EndpointURLMap")
		}

		clone = opts.Clone()
		clone.Credentials.AccessKeyID = "updating"
		if opts.Equal(clone) {
			t.Error("expected not to be equal when updating credentials")
		}
	})
}

func TestRetryer(t *testing.T) {
	minBackoff := 100 * time.Millisecond
	retryer := NewCustomRetryer(2, minBackoff, 1000*time.Millisecond)

	delay, _ := retryer.RetryDelay(3, errors.New("error"))

	if delay < minBackoff {
		t.Errorf("Expected delay to be at least %s, but got %s", minBackoff, delay)
	}
}

func TestSaveWithSSE(t *testing.T) {

}
