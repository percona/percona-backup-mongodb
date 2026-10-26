package oci

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageEndpoint(t *testing.T) {
	privateKey := testPrivateKey(t)
	tests := []struct {
		name          string
		region        string
		endpoint      string
		realmSpecific bool
		wantEndpoint  string
	}{
		{
			name:         "SDK default",
			region:       "eu-frankfurt-1",
			wantEndpoint: "https://objectstorage.eu-frankfurt-1.oraclecloud.com",
		},
		{
			name:         "Alloy endpoint",
			region:       "eu-dcc-rome-1",
			endpoint:     "https://objectstorage.eu-dcc-rome-1.psn-pco.it",
			wantEndpoint: "https://objectstorage.eu-dcc-rome-1.psn-pco.it",
		},
		{
			name:         "namespace-prefixed endpoint",
			region:       "eu-dcc-rome-1",
			endpoint:     "https://testns.objectstorage.eu-dcc-rome-1.example.com",
			wantEndpoint: "https://testns.objectstorage.eu-dcc-rome-1.example.com",
		},
		{
			name:          "override SDK realm-specific endpoint",
			region:        "eu-frankfurt-1",
			endpoint:      "https://storage.example.com",
			realmSpecific: true,
			wantEndpoint:  "https://storage.example.com",
		},
		{
			name:         "trailing slash",
			region:       "eu-dcc-rome-1",
			endpoint:     "https://storage.example.com/",
			wantEndpoint: "https://storage.example.com",
		},
		{
			name:         "HTTP endpoint with port",
			region:       "eu-frankfurt-1",
			endpoint:     "http://localhost:8080",
			wantEndpoint: "http://localhost:8080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(common.OciRealmSpecificServiceEndpointTemplateEnabledEnvVar, strconv.FormatBool(tt.realmSpecific))
			cfg := testConfig(privateKey)
			cfg.Region = tt.region
			cfg.EndpointURL = tt.endpoint
			cfg.Retryer = &Retryer{MaxAttempts: 1}

			client, err := configureClient(cfg)
			require.NoError(t, err)
			httpClient := &endpointCopyHTTPClient{}
			client.HTTPClient = httpClient
			o := &OCI{cfg: cfg, client: client}

			require.NoError(t, o.Copy("source", "destination"))

			assert.Equal(t, []string{
				tt.wantEndpoint + "/n/testns/b/testbucket/actions/copyObject",
				tt.wantEndpoint + "/workRequests/wr1",
			}, httpClient.urls)
			require.NotNil(t, httpClient.details.DestinationRegion)
			assert.Equal(t, tt.region, *httpClient.details.DestinationRegion)
			assert.Equal(t, tt.region, cfg.Region)
		})
	}
}

type endpointCopyHTTPClient struct {
	urls    []string
	details objectstorage.CopyObjectDetails
}

func (c *endpointCopyHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c.urls = append(c.urls, req.URL.String())
	status := http.StatusOK
	header := make(http.Header)
	body := `{"id":"wr1","status":"COMPLETED"}`
	if req.Method == http.MethodPost {
		if err := json.NewDecoder(req.Body).Decode(&c.details); err != nil {
			return nil, err
		}
		status = http.StatusAccepted
		header.Set("opc-work-request-id", "wr1")
		body = ""
	}

	return &http.Response{
		StatusCode: status,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}, nil
}
