package azure

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

func TestClone(t *testing.T) {
	f := 1.1
	c1 := &Config{
		Account:        "acc",
		Container:      "cnt",
		EndpointURL:    "ep.com",
		EndpointURLMap: map[string]string{"n1": "ep1", "n2": "ep2"},
		Prefix:         "p1",
		Credentials: Credentials{
			Key:              "k1",
			WorkloadIdentity: true,
		},
		MaxObjSizeGB: &f,
		Retryer: &Retryer{
			NumMaxRetries: 5,
			MinRetryDelay: 10 * time.Second,
			MaxRetryDelay: 20 * time.Second,
		},
	}

	c2 := c1.Clone()

	if &c1.EndpointURLMap == &c2.EndpointURLMap ||
		c1.MaxObjSizeGB == c2.MaxObjSizeGB ||
		c1.Retryer == c2.Retryer {
		t.Fatal("Deep copy of pointer fields is missing")
	}
	if !reflect.DeepEqual(c1, c2) {
		t.Fatalf("Clone is not performed, diff=%s", cmp.Diff(*c1, *c2))
	}
}

func TestEqual(t *testing.T) {
	f := 1.1
	c1 := &Config{
		Account:        "acc",
		Container:      "cnt",
		EndpointURL:    "ep.com",
		EndpointURLMap: map[string]string{"n1": "ep1", "n2": "ep2"},
		Prefix:         "p1",
		Credentials: Credentials{
			Key: "k1",
		},
		MaxObjSizeGB: &f,
		Retryer: &Retryer{
			NumMaxRetries: 5,
			MinRetryDelay: 10 * time.Second,
			MaxRetryDelay: 20 * time.Second,
		},
	}

	c2 := c1.Clone()

	if !c1.Equal(c2) {
		t.Fatalf("cfg should be equal, diff=%s", cmp.Diff(*c1, *c2))
	}
}

func TestCast(t *testing.T) {
	t.Run("nil config returns error", func(t *testing.T) {
		var c *Config
		if err := c.Cast(); err == nil {
			t.Fatal("expected error for nil config")
		}
	})

	t.Run("empty config applies defaults", func(t *testing.T) {
		c := &Config{}
		if err := c.Cast(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := &Config{
			Retryer: &Retryer{
				NumMaxRetries: defaultMaxRetries,
				MinRetryDelay: defaultMinRetryDelay,
				MaxRetryDelay: defaultMaxRetryDelay,
			},
		}
		if !c.Equal(want) {
			t.Fatalf("wrong config after Cast, diff=%s", cmp.Diff(*c, *want))
		}
	})

	t.Run("workload identity without endpointUrl or account returns error", func(t *testing.T) {
		c := &Config{Credentials: Credentials{WorkloadIdentity: true}}
		if err := c.Cast(); err == nil {
			t.Fatal("expected error when workload identity is set without endpointUrl or account")
		}
	})

	t.Run("workload identity with endpointUrl succeeds", func(t *testing.T) {
		c := &Config{
			EndpointURL: "https://myaccount.blob.core.windows.net",
			Credentials: Credentials{WorkloadIdentity: true},
		}
		if err := c.Cast(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("workload identity with account succeeds", func(t *testing.T) {
		c := &Config{
			Account:     "myaccount",
			Credentials: Credentials{WorkloadIdentity: true},
		}
		if err := c.Cast(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestNew(t *testing.T) {
	t.Run("shared key", func(t *testing.T) {
		cfg := &Config{
			Account:   "myaccount",
			Container: "mycontainer",
			Credentials: Credentials{
				Key: "dGVzdGtleQ==", // base64 encoded — azblob requires valid base64
			},
		}
		_, err := New(cfg, "", nil)
		if err != nil {
			t.Fatalf("unexpected error with shared key credentials: %v", err)
		}
	})

	t.Run("workload identity", func(t *testing.T) {
		cfg := &Config{
			Container:   "mycontainer",
			EndpointURL: "https://myaccount.blob.core.windows.net",
			Credentials: Credentials{
				WorkloadIdentity: true,
			},
		}
		_, err := New(cfg, "", nil)
		if err != nil {
			// NewDefaultAzureCredential itself should not error — it only fails
			// when a token is actually requested. If it does error, the message
			// should relate to credentials, not to a nil pointer or missing account.
			if strings.Contains(err.Error(), "nil") || strings.Contains(err.Error(), "account") {
				t.Fatalf("unexpected error (not a credential error): %v", err)
			}
		}
	})
}

func TestGetMaxObjSizeGB(t *testing.T) {
	tests := []struct {
		name string
		cfg  *Config
		want float64
	}{
		{
			name: "nil MaxObjSizeGB returns default",
			cfg:  &Config{},
			want: defaultMaxObjSizeGB,
		},
		{
			name: "MaxObjSizeGB below lower bound returns default",
			cfg:  &Config{MaxObjSizeGB: storage.Ref(0.5)},
			want: defaultMaxObjSizeGB,
		},
		{
			name: "MaxObjSizeGB at lower bound returns configured value",
			cfg:  &Config{MaxObjSizeGB: storage.Ref(float64(storage.MinValidMaxObjSizeGB))},
			want: storage.MinValidMaxObjSizeGB,
		},
		{
			name: "MaxObjSizeGB above lower bound returns configured value",
			cfg:  &Config{MaxObjSizeGB: storage.Ref(float64(100))},
			want: 100,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.GetMaxObjSizeGB()
			if got != tt.want {
				t.Errorf("GetMaxObjSizeGB: got=%v, want=%v", got, tt.want)
			}
		})
	}
}
