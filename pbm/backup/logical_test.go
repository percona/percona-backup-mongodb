package backup

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-backup-mongodb/pbm/util"
)

func TestGetNamespacesSizeConcurrency(t *testing.T) {
	TestEnv.Reset(t)

	ctx := t.Context()
	nss := make([]string, 0, namespaceStatsConcurrency+1)
	for i := 0; i <= namespaceStatsConcurrency; i++ {
		coll := fmt.Sprintf("c%d", i)
		nss = append(nss, "pbm1700."+coll)
		require.NoError(t, TestEnv.Client.MongoClient().Database("pbm1700").CreateCollection(ctx, coll))
	}

	var active, peak atomic.Int32
	monitor := &event.CommandMonitor{
		Started: func(_ context.Context, evt *event.CommandStartedEvent) {
			if evt.CommandName != "collStats" {
				return
			}

			current := active.Add(1)
			for maximum := peak.Load(); current > maximum; maximum = peak.Load() {
				if peak.CompareAndSwap(maximum, current) {
					break
				}
			}
		},
		Succeeded: func(_ context.Context, evt *event.CommandSucceededEvent) {
			if evt.CommandName == "collStats" {
				active.Add(-1)
			}
		},
		Failed: func(_ context.Context, evt *event.CommandFailedEvent) {
			if evt.CommandName == "collStats" {
				active.Add(-1)
			}
		},
	}
	client, err := mongo.Connect(options.Client().ApplyURI(TestEnv.Brief.URI).SetMonitor(monitor))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Disconnect(context.Background()))
	})

	admin := TestEnv.Client.MongoClient().Database("admin")
	require.NoError(t, admin.RunCommand(ctx, bson.D{
		{"configureFailPoint", "failCommand"},
		{"mode", "alwaysOn"},
		{"data", bson.D{
			{"failCommands", bson.A{"collStats"}},
			{"blockConnection", true},
			{"blockTimeMS", 100},
		}},
	}).Err())
	t.Cleanup(func() {
		require.NoError(t, admin.RunCommand(context.Background(), bson.D{
			{"configureFailPoint", "failCommand"},
			{"mode", "off"},
		}).Err())
	})

	sizes, err := getNamespacesSize(ctx, client, nss)
	require.NoError(t, err)
	require.Len(t, sizes, len(nss))
	require.Equal(t, int32(namespaceStatsConcurrency), peak.Load())
}

func TestMakeConfigsvrDocFilter(t *testing.T) {
	t.Run("selective backup without wildcards", func(t *testing.T) {
		testCases := []struct {
			desc     string
			bcpNS    []string
			docNS    string
			selected bool
		}{
			{
				desc:     "single backup ns, doc selected",
				bcpNS:    []string{"d.c"},
				docNS:    "d.c",
				selected: true,
			},
			{
				desc:     "multiple backup ns, doc selected",
				bcpNS:    []string{"d.c1", "d.c2", "d.c3"},
				docNS:    "d.c2",
				selected: true,
			},
			{
				desc:     "single backup ns, doc not selected",
				bcpNS:    []string{"d.c"},
				docNS:    "x.y",
				selected: false,
			},
			{
				desc:     "single backup ns, doc not selected different coll",
				bcpNS:    []string{"d.c"},
				docNS:    "d.y",
				selected: false,
			},
			{
				desc:     "multiple backup ns, doc not selected",
				bcpNS:    []string{"d.c1", "d.c2", "d.c3"},
				docNS:    "d.c4",
				selected: false,
			},
		}
		for _, tC := range testCases {
			t.Run(tC.desc, func(t *testing.T) {
				docFilter := makeConfigsvrDocFilter(tC.bcpNS, util.NewUUIDChunkSelector())
				res := docFilter(tC.docNS, bson.Raw{})
				if res != tC.selected {
					t.Errorf("want=%t, got=%t, for backup ns: %s and doc ns: %s", tC.selected, res, tC.bcpNS, tC.docNS)
				}
			})
		}
	})

	t.Run("selective backup with wildcards", func(t *testing.T) {
		testCases := []struct {
			desc     string
			bcpNS    []string
			docNS    string
			selected bool
		}{
			{
				desc:     "single backup ns, doc selected",
				bcpNS:    []string{"d.*"},
				docNS:    "d.c",
				selected: true,
			},
			{
				desc:     "multiple backup ns, doc selected",
				bcpNS:    []string{"d1.*", "d2.*", "d3.*"},
				docNS:    "d2.c2",
				selected: true,
			},
			{
				desc:     "single backup ns, doc not selected",
				bcpNS:    []string{"d.*"},
				docNS:    "x.y",
				selected: false,
			},
			{
				desc:     "multiple backup ns, doc not selected",
				bcpNS:    []string{"d1.*", "d2.*", "d3.*"},
				docNS:    "d4.c4",
				selected: false,
			},
		}
		for _, tC := range testCases {
			t.Run(tC.desc, func(t *testing.T) {
				docFilter := makeConfigsvrDocFilter(tC.bcpNS, util.NewUUIDChunkSelector())
				res := docFilter(tC.docNS, bson.Raw{})
				if res != tC.selected {
					t.Errorf("want=%t, got=%t, for backup ns: %s and doc ns: %s", tC.selected, res, tC.bcpNS, tC.docNS)
				}
			})
		}
	})
}
