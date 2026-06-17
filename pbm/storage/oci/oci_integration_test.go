//go:build oci_integration

package oci

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"

	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

type ociIntegrationConfig struct {
	OCI *Config `yaml:"oci"`
}

// TestOCIStorageIntegration runs live OCI Object Storage tests.
//
// Run it explicitly with the oci_integration build tag:
//
//	PBM_OCI_TEST_CONFIG=/path/to/oci-integration.yaml \
//	go test -tags=oci_integration ./pbm/storage/oci \
//	  -run TestOCIStorageIntegration \
//	  -count=1 \
//	  -v \
//	  -timeout=1h
//
// PBM_OCI_TEST_CONFIG is required and must point to a YAML file with OCI
// backend settings under the oci key:
//
//	oci:
//	  region: eu-frankfurt-1
//	  namespace: <object-storage-namespace>
//	  bucket: <bucket-name>
//	  prefix: <dedicated-test-prefix>
//	  credentials:
//	    type: userPrincipal
//	    userPrincipal:
//	      ...
//
// The prefix must be non-empty and dedicated to this test. The test always
// deletes all OCI objects that match the prefix before running. It also deletes
// them after the test completes unless PBM_OCI_TEST_CLEANUP=0 is set.
//
// Large split-merge cases are skipped by default. Enable them explicitly with
// PBM_OCI_TEST_RUN_LARGE=1 and PBM_OCI_TEST_RUN_XLARGE=1.
//
// Cleanup uses raw OCI SDK list/delete calls, so orphaned PBM split-merge parts
// from interrupted runs are removed as physical objects.
func TestOCIStorageIntegration(t *testing.T) {
	cfgPath := os.Getenv("PBM_OCI_TEST_CONFIG")
	cleanupAfter := os.Getenv("PBM_OCI_TEST_CLEANUP") != "0"
	require.NotEmpty(t, cfgPath, "set PBM_OCI_TEST_CONFIG to an OCI integration config file to run live OCI storage tests")

	cfg, err := readOCIIntegrationConfig(cfgPath)
	require.NoError(t, err)
	require.NotNil(t, cfg.OCI, "PBM_OCI_TEST_CONFIG must include oci settings")
	require.NotEmpty(t, cfg.OCI.Prefix, "OCI integration test requires a dedicated non-empty prefix")

	cleanupOCIIntegrationPrefix(t, cfg.OCI.Clone())
	if cleanupAfter {
		t.Cleanup(func() {
			cleanupOCIIntegrationPrefix(t, cfg.OCI.Clone())
		})
	}

	stg, err := New(cfg.OCI.Clone(), "node", nil)
	require.NoError(t, err)

	splitMergeCfg := ociSplitMergeMWTestConfig()
	storage.RunStorageBaseTests(t, stg, storage.OCI)
	storage.RunStorageAPITests(t, stg)
	storage.RunSplitMergeMWTestsWithConfig(t, stg, splitMergeCfg)

	t.Run("with downloader", func(t *testing.T) {
		stg, err := NewWithDownloader(cfg.OCI.Clone(), "node", nil, 0, 0, 0)
		require.NoError(t, err)

		storage.RunStorageBaseTests(t, stg, storage.OCI)
		storage.RunStorageAPITests(t, stg)
		storage.RunSplitMergeMWTestsWithConfig(t, stg, splitMergeCfg)
	})
}

func readOCIIntegrationConfig(path string) (ociIntegrationConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return ociIntegrationConfig{}, err
	}

	var cfg ociIntegrationConfig
	err = yaml.Unmarshal(b, &cfg)
	if err != nil {
		return ociIntegrationConfig{}, err
	}
	if cfg.OCI != nil {
		cfg.OCI.Bucket = storage.TrimSlashes(cfg.OCI.Bucket)
		cfg.OCI.Prefix = storage.TrimSlashes(cfg.OCI.Prefix)
	}

	return cfg, nil
}

func ociSplitMergeMWTestConfig() storage.SplitMergeMWTestConfig {
	return storage.SplitMergeMWTestConfig{
		SkipLarge:      os.Getenv("PBM_OCI_TEST_RUN_LARGE") != "1",
		SkipExtraLarge: os.Getenv("PBM_OCI_TEST_RUN_XLARGE") != "1",
	}
}

func cleanupOCIIntegrationPrefix(t *testing.T, cfg *Config) {
	t.Helper()

	require.NoError(t, cfg.Cast())
	client, err := configureClient(cfg)
	require.NoError(t, err)

	prefix := storage.TrimSlashes(cfg.Prefix)
	require.NotEmpty(t, prefix, "OCI integration cleanup requires a dedicated non-empty prefix")

	ctx := context.Background()
	start := time.Now()
	objectNames, err := listTestObjects(ctx, client, cfg, prefix)
	require.NoError(t, err)
	t.Logf("OCI cleanup: found %d objects under prefix %q", len(objectNames), prefix)

	deleteStart := time.Now()
	deleteFailures := 0
	for i, name := range objectNames {
		err := deleteTestObject(ctx, client, cfg, name)
		if !assert.NoError(t, err, "delete %q", name) {
			deleteFailures++
		}
		if (i+1)%100 == 0 || i+1 == len(objectNames) {
			t.Logf("OCI cleanup: processed %d/%d objects, failures=%d, elapsed=%s",
				i+1, len(objectNames), deleteFailures, time.Since(deleteStart))
		}
	}
	t.Logf("OCI cleanup: finished prefix %q, objects=%d, failures=%d, elapsed=%s",
		prefix, len(objectNames), deleteFailures, time.Since(start))
}

func listTestObjects(
	ctx context.Context,
	client *objectstorage.ObjectStorageClient,
	cfg *Config,
	prefix string,
) ([]string, error) {
	var objectNames []string
	var start *string
	for {
		res, err := client.ListObjects(ctx, objectstorage.ListObjectsRequest{
			NamespaceName: common.String(cfg.Namespace),
			BucketName:    common.String(cfg.Bucket),
			Prefix:        common.String(prefix),
			Start:         start,
			Fields:        common.String("name"),
		})
		if err != nil {
			return nil, err
		}

		for _, obj := range res.Objects {
			if obj.Name == nil {
				continue
			}
			name := *obj.Name
			if name == prefix || strings.HasPrefix(name, prefix+"/") {
				objectNames = append(objectNames, name)
			}
		}
		if res.NextStartWith == nil {
			break
		}
		start = res.NextStartWith
	}

	return objectNames, nil
}

func deleteTestObject(ctx context.Context, client *objectstorage.ObjectStorageClient, cfg *Config, name string) error {
	_, err := client.DeleteObject(ctx, objectstorage.DeleteObjectRequest{
		NamespaceName: common.String(cfg.Namespace),
		BucketName:    common.String(cfg.Bucket),
		ObjectName:    common.String(name),
	})
	if err != nil && isNotFound(err) {
		return nil
	}

	return err
}
