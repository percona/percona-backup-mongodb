package backup

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/percona/percona-backup-mongodb/pbm/defs"
	stds3 "github.com/percona/percona-backup-mongodb/pbm/storage/s3"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
)

func TestMetadataEncodeDecodeWithMinio(t *testing.T) {
	ctx := context.Background()

	minioContainer, err := minio.Run(ctx, "pgsty/silo:RELEASE.2026-09-03T13-18-01Z")
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

	expected := prepareBackupList(t, backups)
	actual, err := BackupsList(t.Context(), TestEnv.Client, 0)

	assert.NoError(t, err)
	assertExpectedBackupList(t, expected, actual)
}

func TestChangeRSStateOrAdd(t *testing.T) {
	const (
		backupName = "incremental"
		rsName     = "shard2RS"
		node       = "mongo-pbm-test:27027"
		failure    = "can't find incremental backup history"
	)

	TestEnv.Reset(t)
	_, err := TestEnv.Client.BcpCollection().InsertOne(t.Context(), BackupMeta{
		Name:     backupName,
		Type:     defs.IncrementalBackup,
		Status:   defs.StatusStarting,
		Replsets: []BackupReplset{},
	})
	require.NoError(t, err)

	err = ChangeRSStateOrAdd(TestEnv.Client, backupName, BackupReplset{
		Name:    rsName,
		Node:    node,
		StartTS: 1,
		Status:  defs.StatusRunning,
	}, defs.StatusError, failure)
	require.NoError(t, err)

	meta, err := NewDBManager(TestEnv.Client).GetBackupByName(t.Context(), backupName)
	require.NoError(t, err)
	require.Len(t, meta.Replsets, 1)
	rs := meta.Replsets[0]
	assert.Equal(t, rsName, rs.Name)
	assert.Equal(t, node, rs.Node)
	assert.Equal(t, defs.StatusError, rs.Status)
	assert.Equal(t, failure, rs.Error)
	require.Len(t, rs.Conditions, 1)
	assert.Equal(t, Condition{
		Timestamp: rs.LastTransitionTS,
		Status:    defs.StatusError,
		Error:     failure,
	}, rs.Conditions[0])
}

func TestChangeRSStateOrAddMatchesChangeRSState(t *testing.T) {
	const (
		changeBackupName = "change-state"
		orAddBackupName  = "change-state-or-add"
		rsName           = "shard2RS"
		failure          = "backup failed"
	)

	TestEnv.Reset(t)
	rs := BackupReplset{
		Name:             rsName,
		Node:             "mongo-pbm-test:27027",
		Status:           defs.StatusRunning,
		StartTS:          1,
		LastTransitionTS: 1,
		CustomThisID:     "existing-id",
		Conditions: []Condition{{
			Timestamp: 1,
			Status:    defs.StatusRunning,
		}},
	}

	for _, name := range []string{changeBackupName, orAddBackupName} {
		_, err := TestEnv.Client.BcpCollection().InsertOne(t.Context(), BackupMeta{
			Name:     name,
			Type:     defs.IncrementalBackup,
			Status:   defs.StatusStarting,
			Replsets: []BackupReplset{rs},
		})
		require.NoError(t, err)
	}

	err := ChangeRSState(TestEnv.Client, changeBackupName, rsName, defs.StatusError, failure)
	require.NoError(t, err)
	err = ChangeRSStateOrAdd(TestEnv.Client, orAddBackupName, rs, defs.StatusError, failure)
	require.NoError(t, err)

	changed, err := NewDBManager(TestEnv.Client).GetBackupByName(t.Context(), changeBackupName)
	require.NoError(t, err)
	orAdded, err := NewDBManager(TestEnv.Client).GetBackupByName(t.Context(), orAddBackupName)
	require.NoError(t, err)
	require.Len(t, changed.Replsets, 1)
	require.Len(t, orAdded.Replsets, 1)
	changedRS := changed.Replsets[0]
	orAddedRS := orAdded.Replsets[0]
	require.Len(t, changedRS.Conditions, 2)
	require.Len(t, orAddedRS.Conditions, 2)
	assert.Equal(
		t,
		changedRS.LastTransitionTS,
		changedRS.Conditions[1].Timestamp,
	)
	assert.Equal(
		t,
		orAddedRS.LastTransitionTS,
		orAddedRS.Conditions[1].Timestamp,
	)

	diff := cmp.Diff(
		changedRS,
		orAddedRS,
		cmpopts.IgnoreFields(BackupReplset{}, "LastTransitionTS"),
		cmpopts.IgnoreFields(Condition{}, "Timestamp"),
	)
	assert.Empty(t, diff)
}

func TestConvergedReturnsReplicaSetError(t *testing.T) {
	const (
		backupName = "incremental"
		rsName     = "shard2RS"
		failure    = "can't find incremental backup history"
	)

	TestEnv.Reset(t)
	_, err := TestEnv.Client.BcpCollection().InsertOne(t.Context(), BackupMeta{
		Name:   backupName,
		Type:   defs.IncrementalBackup,
		Status: defs.StatusStarting,
		Replsets: []BackupReplset{{
			Name:   rsName,
			Status: defs.StatusError,
			Error:  failure,
		}},
	})
	require.NoError(t, err)

	b := &Backup{leadConn: TestEnv.Client}
	ok, err := b.converged(
		t.Context(),
		backupName,
		"opid",
		[]topo.Shard{{RS: rsName}},
		defs.StatusRunning,
	)

	assert.False(t, ok)
	require.EqualError(t, err, "backup on shard shard2RS failed: can't find incremental backup history")
}

func assertExpectedBackupList(t *testing.T, expectedMeta, actualMeta []BackupMeta) {
	t.Helper()
	expected := bcpNames(expectedMeta)
	actual := bcpNames(actualMeta)
	assert.ElementsMatch(t, expected, actual)
}

func prepareBackupList(t *testing.T, backups map[string][]bcp) []BackupMeta {
	var inserted []BackupMeta

	for profile, bcps := range backups {
		stg := TestEnv.PbmStorage
		if profile != "" {
			stg = TempStorageProfile(t, profile)
		}

		for _, bcp := range bcps {
			meta := insertTestBcpMeta(t, TestEnv, stg, bcp)
			inserted = append(inserted, meta)
		}
	}

	return inserted
}

func bcpNames(backups []BackupMeta) []string {
	var names []string
	for _, b := range backups {
		names = append(names, b.Name)
	}
	return names
}
