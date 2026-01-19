package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	stds3 "github.com/percona/percona-backup-mongodb/pbm/storage/s3"
	"github.com/stretchr/testify/assert"
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

	defaultConfig, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithBaseEndpoint(endpoint),
		awsconfig.WithCredentialsProvider(
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

func TestBackupsList(t *testing.T) {
	TestEnv.Reset(t)
	now := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)

	backups := map[string][]bcp{
		"": {
			{Name: "a1", LWT: now.Add(-20 * time.Minute)},
			{Name: "a2", LWT: now.Add(-15 * time.Minute)},
			{Name: "a3", LWT: now.Add(-10 * time.Minute)},
		},
		"other": {
			{Name: "b1", LWT: now.Add(-15 * time.Minute)},
			{Name: "b2", LWT: now.Add(-10 * time.Minute)},
		},
	}

	expectedNames := prepareBackupList(t, backups)

	for profile := range backups {
		t.Run(fmt.Sprintf("List backups in profile %s", profile), func(t *testing.T) {
			testExpectedList(t, profile, expectedNames[profile])
		})
	}

	t.Run("List all backups", func(t *testing.T) {
		testExpectedList(t, "*", expectedNames["*"])
	})
}

func testExpectedList(t *testing.T, p string, expected []string) {
	profile := config.NewProfileName(p)
	list, err := BackupsList(t.Context(), TestEnv.Client, profile, 0)
	assert.NoError(t, err)
	actual := bcpNames(list)
	assert.ElementsMatchf(t, expected, actual, "Expectged backups %v, got %v", expected, actual)
}

func prepareBackupList(t *testing.T, backups map[string][]bcp) map[string][]string {
	names := make(map[string][]string)

	for profile, bcps := range backups {
		stg := TestEnv.PbmStorage
		if profile != "" {
			stg = TempStorageProfile(t, profile)
		}

		var expected []BackupMeta
		for _, bcp := range bcps {
			meta := insertTestBcpMeta(t, TestEnv, stg, bcp)
			expected = append(expected, meta)
		}

		pnames := bcpNames(expected)
		names[profile] = append(names[profile], pnames...)
		names["*"] = append(names["*"], pnames...)
	}

	return names
}

func bcpNames(backups []BackupMeta) []string {
	var names []string
	for _, b := range backups {
		names = append(names, b.Name)
	}
	return names
}
