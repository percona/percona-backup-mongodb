package mio

import (
	"reflect"
	"runtime"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

func TestClone(t *testing.T) {
	f := 1.1
	c1 := &Config{
		Region:      "eu",
		Endpoint:    "ep.com",
		EndpointMap: map[string]string{"n1": "ep1", "n2": "ep2"},
		Bucket:      "b1",
		Prefix:      "p1",
		Credentials: Credentials{
			AccessKeyID:     "k1",
			SecretAccessKey: "k2",
			SessionToken:    "sess",
		},
		Secure:       true,
		PartSize:     6 << 20,
		MaxObjSizeGB: &f,
		Retryer: &Retryer{
			NumMaxRetries: 1,
		},
	}

	c2 := c1.Clone()

	if &c1.EndpointMap == &c2.EndpointMap ||
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
		Region:      "eu",
		Endpoint:    "ep.com",
		EndpointMap: map[string]string{"n1": "ep1", "n2": "ep2"},
		Bucket:      "b1",
		Prefix:      "p1",
		Credentials: Credentials{
			AccessKeyID:     "k1",
			SecretAccessKey: "k2",
			SessionToken:    "sess",
		},
		Secure:       true,
		PartSize:     6 << 20,
		MaxObjSizeGB: &f,
		Retryer: &Retryer{
			NumMaxRetries: 1,
		},
	}

	c2 := c1.Clone()

	if !c1.Equal(c2) {
		t.Fatalf("cfg should be equal, diff=%s", cmp.Diff(*c1, *c2))
	}
}

func TestCast(t *testing.T) {
	var c *Config
	err := c.Cast()
	if err == nil {
		t.Fatal("sigsegv should have happened instead")
	}

	c = &Config{}
	if err := c.Cast(); err == nil {
		t.Fatal("want error when EndpointURL is not specified")
	}

	c.Endpoint = "url"
	err = c.Cast()
	if err != nil {
		t.Fatalf("got error during Cast: %v", err)
	}
	defaultConcurrency := defaultUploadConcurrency(runtime.NumCPU())
	want := &Config{
		Endpoint:          "url",
		PartSize:          defaultPartSize,
		UploadConcurrency: defaultConcurrency,
		Retryer: &Retryer{
			NumMaxRetries: defaultMaxRetries,
		},
	}

	if !c.Equal(want) {
		t.Fatalf("wrong config after Cast, diff=%s", cmp.Diff(*c, *want))
	}

	for _, tt := range []struct {
		value int
		want  int
	}{
		{-1, defaultConcurrency},
		{0, defaultConcurrency},
		{1, 1},
		{4, 4},
		{8, 8},
	} {
		c.UploadConcurrency = tt.value
		if err := c.Cast(); err != nil {
			t.Fatalf("Cast with uploadConcurrency %d: %v", tt.value, err)
		}
		if c.UploadConcurrency != tt.want {
			t.Fatalf("uploadConcurrency %d: got %d, want %d", tt.value, c.UploadConcurrency, tt.want)
		}
	}
}

func TestDefaultUploadConcurrency(t *testing.T) {
	for _, tt := range []struct {
		numCPU int
		want   int
	}{
		{1, 1},
		{2, 1},
		{3, 1},
		{4, 2},
		{5, 2},
		{6, 2},
		{12, 2},
		{128, 2},
	} {
		if got := defaultUploadConcurrency(tt.numCPU); got != tt.want {
			t.Errorf("default concurrency for %d CPUs: got %d, want %d", tt.numCPU, got, tt.want)
		}
	}
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
