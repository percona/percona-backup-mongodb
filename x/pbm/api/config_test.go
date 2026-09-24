package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	tcetcd "github.com/testcontainers/testcontainers-go/modules/etcd"

	"github.com/percona/percona-backup-mongodb/x/pbm/config"
	"github.com/percona/percona-backup-mongodb/x/pbm/config/fs"
	"github.com/percona/percona-backup-mongodb/x/pbm/storage"
)

const etcdImage = "gcr.io/etcd-development/etcd:v3.6.12"

var testEndpoints []string

func TestMain(m *testing.M) {
	ctx := context.Background()

	ctr, err := tcetcd.Run(ctx, etcdImage)
	if err != nil {
		log.Fatalf("start etcd container: %v", err)
	}

	testEndpoints, err = ctr.ClientEndpoints(ctx)
	if err != nil {
		_ = ctr.Terminate(ctx)
		log.Fatalf("get etcd client endpoints: %v", err)
	}

	code := m.Run()

	if err := ctr.Terminate(ctx); err != nil {
		log.Printf("terminate etcd container: %v", err)
	}

	os.Exit(code)
}

func newTestHandler(t *testing.T) http.Handler {
	t.Helper()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   testEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("dial etcd client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	// Isolate from other tests: "\x00" + WithFromKey spans every key.
	if _, err := cli.Delete(t.Context(), "\x00", clientv3.WithFromKey()); err != nil {
		t.Fatalf("reset etcd: %v", err)
	}

	mux := http.NewServeMux()
	newConfigHandler(config.New(cli, noopResyncer{})).registerRoutes(mux)
	return mux
}

// noopResyncer satisfies config's storage-resyncer dependency; these tests
// exercise the HTTP layer, not backup resyncing.
type noopResyncer struct{}

func (noopResyncer) Resync(context.Context, *config.StorageConf) error { return nil }

func serve(h http.Handler, r *http.Request) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	return rr
}

func putReq(t *testing.T, name string, cfg *config.Config) *http.Request {
	t.Helper()

	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	return httptest.NewRequest(http.MethodPut, "/config/"+name, bytes.NewReader(b))
}

func testConfig(name, path string) *config.Config {
	return &config.Config{
		Name: name,
		Storage: config.StorageConf{
			Type:       storage.Filesystem,
			Filesystem: &fs.Config{Path: path},
		},
	}
}

func TestHandleGetAll(t *testing.T) {
	h := newTestHandler(t)

	for _, name := range []string{"b", "a"} {
		if rr := serve(h, putReq(t, name, testConfig(name, "/"+name))); rr.Code != http.StatusNoContent {
			t.Fatalf("PUT %q: code = %d, want 204", name, rr.Code)
		}
	}

	rr := serve(h, httptest.NewRequest(http.MethodGet, "/config", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rr.Code)
	}
	var got []*config.Config
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(got) != 2 || got[0].Name != "a" || got[1].Name != "b" {
		t.Fatalf("unexpected body: %+v", got)
	}
}

func TestHandleGet(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		h := newTestHandler(t)

		if rr := serve(h, putReq(t, "main", testConfig("main", "/data"))); rr.Code != http.StatusNoContent {
			t.Fatalf("PUT: code = %d, want 204", rr.Code)
		}

		rr := serve(h, httptest.NewRequest(http.MethodGet, "/config/main", nil))

		if rr.Code != http.StatusOK {
			t.Fatalf("code = %d, want 200", rr.Code)
		}
		var got config.Config
		if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if got.Name != "main" {
			t.Fatalf("Name = %q, want main", got.Name)
		}
	})

	t.Run("not found", func(t *testing.T) {
		h := newTestHandler(t)

		rr := serve(h, httptest.NewRequest(http.MethodGet, "/config/ghost", nil))

		if rr.Code != http.StatusNotFound {
			t.Fatalf("code = %d, want 404", rr.Code)
		}
	})
}

func TestHandleUpsert(t *testing.T) {
	t.Run("stores body under path name", func(t *testing.T) {
		h := newTestHandler(t)

		body := `{"name":"ignored","storage":{"type":"filesystem","filesystem":{"path":"/data"}}}`
		rr := serve(h, httptest.NewRequest(http.MethodPut, "/config/main", strings.NewReader(body)))
		if rr.Code != http.StatusNoContent {
			t.Fatalf("code = %d, want 204", rr.Code)
		}

		rr = serve(h, httptest.NewRequest(http.MethodGet, "/config/main", nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("GET code = %d, want 200", rr.Code)
		}
		var got config.Config
		if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if got.Name != "main" {
			t.Fatalf("Name = %q, want main (path is authoritative)", got.Name)
		}
		if got.Storage.Filesystem == nil || got.Storage.Filesystem.Path != "/data" {
			t.Fatalf("storage = %+v", got.Storage.Filesystem)
		}
	})

	t.Run("rejects invalid JSON", func(t *testing.T) {
		h := newTestHandler(t)

		rr := serve(h, httptest.NewRequest(http.MethodPut, "/config/main", strings.NewReader("not json")))
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("code = %d, want 400", rr.Code)
		}

		rr = serve(h, httptest.NewRequest(http.MethodGet, "/config/main", nil))
		if rr.Code != http.StatusNotFound {
			t.Fatalf("GET code = %d, want 404", rr.Code)
		}
	})
}

func TestHandleDelete(t *testing.T) {
	t.Run("existing", func(t *testing.T) {
		h := newTestHandler(t)

		if rr := serve(h, putReq(t, "tmp", testConfig("tmp", "/a"))); rr.Code != http.StatusNoContent {
			t.Fatalf("PUT: code = %d, want 204", rr.Code)
		}

		rr := serve(h, httptest.NewRequest(http.MethodDelete, "/config/tmp", nil))
		if rr.Code != http.StatusNoContent {
			t.Fatalf("DELETE code = %d, want 204", rr.Code)
		}

		rr = serve(h, httptest.NewRequest(http.MethodGet, "/config/tmp", nil))
		if rr.Code != http.StatusNotFound {
			t.Fatalf("GET after delete code = %d, want 404", rr.Code)
		}
	})

	t.Run("not found", func(t *testing.T) {
		h := newTestHandler(t)

		rr := serve(h, httptest.NewRequest(http.MethodDelete, "/config/ghost", nil))
		if rr.Code != http.StatusNotFound {
			t.Fatalf("code = %d, want 404", rr.Code)
		}
	})
}

func TestHandleResync(t *testing.T) {
	t.Run("existing", func(t *testing.T) {
		h := newTestHandler(t)

		if rr := serve(h, putReq(t, "main", testConfig("main", "/data"))); rr.Code != http.StatusNoContent {
			t.Fatalf("PUT: code = %d, want 204", rr.Code)
		}

		rr := serve(h, httptest.NewRequest(http.MethodPost, "/config/main/resync", nil))
		if rr.Code != http.StatusNoContent {
			t.Fatalf("code = %d, want 204", rr.Code)
		}
	})

	t.Run("not found", func(t *testing.T) {
		h := newTestHandler(t)

		rr := serve(h, httptest.NewRequest(http.MethodPost, "/config/ghost/resync", nil))
		if rr.Code != http.StatusNotFound {
			t.Fatalf("code = %d, want 404", rr.Code)
		}
	})
}
