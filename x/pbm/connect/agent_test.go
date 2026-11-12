package connect

import (
	"context"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

const mongoImage = "percona/percona-server-mongodb:8.0"

// unreachableURI points at a closed port; the short server selection timeout
// keeps the failing connect attempt under a second.
const unreachableURI = "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=500"

func TestMain(m *testing.M) {
	code := m.Run()

	if rs.ctr != nil {
		if err := rs.ctr.Terminate(context.Background()); err != nil {
			log.Printf("terminate mongodb container: %v", err)
		}
	}

	os.Exit(code)
}

func TestConnectAgent(t *testing.T) {
	ctx := context.Background()

	t.Run("mongod is down", func(t *testing.T) {
		t.Run("uri and name are empty", func(t *testing.T) {
			lg, _ := newObservedLogger()

			ac, err := ConnectAgent(ctx, lg, "", "")
			if err == nil {
				t.Fatalf("ConnectAgent() = %+v, want error", ac)
			}
		})

		t.Run("detached agent", func(t *testing.T) {
			lg, _ := newObservedLogger()

			ac, err := ConnectAgent(ctx, lg, "", "agent-1")
			if err != nil {
				t.Fatalf("ConnectAgent: %v", err)
			}
			defer ac.Disconnect(ctx, lg)

			if ac.Name != "agent-1" {
				t.Errorf("Name = %q, want %q", ac.Name, "agent-1")
			}
			if ac.NodeConn != nil || ac.LeadConn != nil {
				t.Errorf("connections = (%v, %v), want both nil", ac.NodeConn, ac.LeadConn)
			}
		})

		t.Run("mongod is unreachable and name is empty", func(t *testing.T) {
			lg, _ := newObservedLogger()

			ac, err := ConnectAgent(ctx, lg, unreachableURI, "")
			if err == nil {
				t.Fatalf("ConnectAgent() = %+v, want error", ac)
			}
			wantErr := "infer agent name from mongod"
			if !strings.Contains(err.Error(), wantErr) {
				t.Errorf("error = %q, want=%q", err, wantErr)
			}
		})

		t.Run("mongod is unreachable and name is given", func(t *testing.T) {
			lg, logs := newObservedLogger()

			ac, err := ConnectAgent(ctx, lg, unreachableURI, "agent-1")
			if err != nil {
				t.Fatalf("ConnectAgent: %v", err)
			}
			defer ac.Disconnect(ctx, lg)

			if ac.Name != "agent-1" {
				t.Errorf("Name = %q, want %q", ac.Name, "agent-1")
			}
			if ac.NodeConn != nil || ac.LeadConn != nil {
				t.Errorf("connections = (%v, %v), want both nil", ac.NodeConn, ac.LeadConn)
			}
			if n := logs.FilterLevelExact(zapcore.ErrorLevel).Len(); n != 1 {
				t.Errorf("error logs = %d, want 1", n)
			}
		})
	})

	t.Run("rs is running", func(t *testing.T) {
		uri := rsURI(t)
		me := helloMe(t, uri)

		t.Run("name is inferred from mongod", func(t *testing.T) {
			lg, _ := newObservedLogger()

			ac, err := ConnectAgent(ctx, lg, uri, "")
			if err != nil {
				t.Fatalf("ConnectAgent: %v", err)
			}
			defer ac.Disconnect(ctx, lg)

			if ac.Name != me {
				t.Errorf("Name = %q, want %q", ac.Name, me)
			}
			if err := pingErr(ac.NodeConn); err != nil {
				t.Errorf("ping node: %v", err)
			}
			if err := pingErr(ac.LeadConn.MongoClient()); err != nil {
				t.Errorf("ping leader: %v", err)
			}
		})

		t.Run("name matches mongod", func(t *testing.T) {
			lg, logs := newObservedLogger()

			ac, err := ConnectAgent(ctx, lg, uri, me)
			if err != nil {
				t.Fatalf("ConnectAgent: %v", err)
			}
			defer ac.Disconnect(ctx, lg)

			if ac.Name != me {
				t.Errorf("Name = %q, want %q", ac.Name, me)
			}
			if n := logs.FilterLevelExact(zapcore.WarnLevel).Len(); n != 0 {
				t.Errorf("warn logs = %d, want 0", n)
			}
		})

		t.Run("name differs from mongod", func(t *testing.T) {
			lg, logs := newObservedLogger()

			ac, err := ConnectAgent(ctx, lg, uri, "agent-1")
			if err != nil {
				t.Fatalf("ConnectAgent: %v", err)
			}
			defer ac.Disconnect(ctx, lg)

			if ac.Name != "agent-1" {
				t.Errorf("Name = %q, want %q", ac.Name, "agent-1")
			}
			if ac.NodeConn == nil || ac.LeadConn == nil {
				t.Errorf("connections = (%v, %v), want both set", ac.NodeConn, ac.LeadConn)
			}
			warns := logs.FilterLevelExact(zapcore.WarnLevel).All()
			if len(warns) != 1 {
				t.Fatalf("warn logs = %d, want 1", len(warns))
			}
			if got := warns[0].ContextMap()["mongod-name"]; got != me {
				t.Errorf("mongod-name = %v, want %q", got, me)
			}
		})

		t.Run("disconnect closes both connections", func(t *testing.T) {
			lg, _ := newObservedLogger()

			ac, err := ConnectAgent(ctx, lg, uri, "")
			if err != nil {
				t.Fatalf("ConnectAgent: %v", err)
			}

			ac.Disconnect(ctx, lg)

			if err := pingErr(ac.NodeConn); err == nil {
				t.Error("ping node after disconnect: want error")
			}
			if err := pingErr(ac.LeadConn.MongoClient()); err == nil {
				t.Error("ping leader after disconnect: want error")
			}
		})
	})
}

var rs struct {
	once sync.Once
	ctr  *tcmongo.MongoDBContainer
	uri  string
	err  error
}

// rsURI returns the connection string of a shared single-node replica set,
// starting its container on first use.
func rsURI(t *testing.T) string {
	t.Helper()

	rs.once.Do(func() {
		ctx := context.Background()
		rs.ctr, rs.err = tcmongo.Run(ctx, mongoImage, tcmongo.WithReplicaSet("rs0"))
		if rs.err != nil {
			return
		}
		rs.uri, rs.err = directURI(ctx, rs.ctr)
	})
	if rs.err != nil {
		t.Fatalf("start mongodb replica set: %v", rs.err)
	}

	return rs.uri
}

// directURI returns a container connection string with a direct connection.
// The replica set member is advertised under the container IP, which is not
// reachable from the host, so the driver must not follow the topology.
func directURI(ctx context.Context, ctr *tcmongo.MongoDBContainer) (string, error) {
	s, err := ctr.ConnectionString(ctx)
	if err != nil {
		return "", err
	}

	u, err := url.Parse(s)
	if err != nil {
		return "", err
	}
	u.RawQuery = url.Values{"directConnection": {"true"}}.Encode()

	return u.String(), nil
}

// helloMe returns the mongod name reported by the hello command.
func helloMe(t *testing.T, uri string) string {
	t.Helper()

	ctx := context.Background()
	c, err := MongoConnect(ctx, uri, Direct(true), NoRS())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = c.Disconnect(ctx) }()

	inf, err := getNodeInfo(ctx, c)
	if err != nil {
		t.Fatalf("get node info: %v", err)
	}

	return inf.Me
}

func newObservedLogger() (*zap.Logger, *observer.ObservedLogs) {
	core, logs := observer.New(zapcore.InfoLevel)
	return zap.New(core), logs
}

func pingErr(c *mongo.Client) error {
	return c.Ping(context.Background(), nil)
}
