package oci

import (
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

func TestSSEHeaders(t *testing.T) {
	const kmsKeyID = "ocid1.key.oc1..test"
	tests := []struct {
		name            string
		sse             SSE
		wantKMSKeyID    string
		wantSSECustomer bool
		wantError       string
	}{
		{
			name: "default encryption",
		},
		{
			name:         "kms",
			sse:          SSE{KmsKeyID: kmsKeyID},
			wantKMSKeyID: kmsKeyID,
		},
		{
			name:            "sse-c",
			sse:             testSSECustomerConfig(),
			wantSSECustomer: true,
		},
		{
			name: "kms and sse-c",
			sse: SSE{
				KmsKeyID:       kmsKeyID,
				SseCustomerKey: testSSECustomerConfig().SseCustomerKey,
			},
			wantError: "kmsKeyID cannot be used with SSE-C",
		},
		{
			name: "invalid sse-c key",
			sse: SSE{
				SseCustomerKey: storage.MaskedString("invalid"),
			},
			wantError: "decode sseCustomerKey",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers, err := tt.sse.headers()

			if tt.wantError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantError)
				return
			}
			require.NoError(t, err)

			if tt.wantKMSKeyID != "" {
				require.NotNil(t, headers.kmsKeyID)
				assert.Equal(t, tt.wantKMSKeyID, *headers.kmsKeyID)
			} else {
				assert.Nil(t, headers.kmsKeyID)
			}

			if tt.wantSSECustomer {
				assertSSECustomerRequest(t, headers.customerAlgorithm, headers.customerKey, headers.customerKeySHA256)
			} else {
				assert.Nil(t, headers.customerAlgorithm)
				assert.Nil(t, headers.customerKey)
				assert.Nil(t, headers.customerKeySHA256)
			}
		})
	}
}

func testSSECustomerConfig() SSE {
	return SSE{
		SseCustomerKey: storage.MaskedString(base64.StdEncoding.EncodeToString([]byte("0123456789abcdefghijklmnopqrstuv"))),
	}
}

func assertSSECustomerHTTPHeaders(t *testing.T, header http.Header) {
	t.Helper()

	assert.Equal(t, sseCustomerAlgorithm, header.Get("opc-sse-customer-algorithm"))
	assert.Equal(t, string(testSSECustomerConfig().SseCustomerKey), header.Get("opc-sse-customer-key"))
	assert.Equal(t, testSSECustomerKeySHA256(), header.Get("opc-sse-customer-key-sha256"))
}

func assertSSECustomerRequest(t *testing.T, algorithm, key, keySHA256 *string) {
	t.Helper()

	require.NotNil(t, algorithm)
	assert.Equal(t, sseCustomerAlgorithm, *algorithm)
	require.NotNil(t, key)
	assert.Equal(t, string(testSSECustomerConfig().SseCustomerKey), *key)
	require.NotNil(t, keySHA256)
	assert.Equal(t, testSSECustomerKeySHA256(), *keySHA256)
}

func testSSECustomerKeySHA256() string {
	key, _ := base64.StdEncoding.DecodeString(string(testSSECustomerConfig().SseCustomerKey))
	sum := sha256.Sum256(key)
	return base64.StdEncoding.EncodeToString(sum[:])
}
