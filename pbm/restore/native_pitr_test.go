package restore

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/mongodb/mongo-tools/common/db"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/storage/fs"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
)

// These tests don't need a mongod: go test ./pbm/restore -bench=NONE -run NativePITR

func nts(t, i uint32) bson.Timestamp { return bson.Timestamp{T: t, I: i} }

func chunk(name string, start, end bson.Timestamp) nativePITRChunk {
	return nativePITRChunk{fname: name, start: start, end: end}
}

func chainNames(c []nativePITRChunk) []string {
	names := make([]string, len(c))
	for i := range c {
		names[i] = c[i].fname
	}
	return names
}

func TestNativePITRChain(t *testing.T) {
	// adjacent PBM chunks share their boundary entry
	a := chunk("a", nts(100, 1), nts(200, 5))
	b := chunk("b", nts(200, 5), nts(300, 2))
	c := chunk("c", nts(300, 2), nts(400, 9))

	cases := []struct {
		name     string
		chunks   []nativePITRChunk
		from, to bson.Timestamp
		want     []string
		wantErr  bool
	}{
		{"contiguous, unsorted input", []nativePITRChunk{c, a, b}, nts(150, 0), nts(350, 0), []string{"a", "b", "c"}, false},
		// a chunk includes its start entry (slicer copies ts >= start)
		{"from on a boundary", []nativePITRChunk{a, b, c}, nts(200, 5), nts(250, 0), []string{"b"}, false},
		{"to on a boundary", []nativePITRChunk{a, b, c}, nts(150, 0), nts(300, 2), []string{"a", "b"}, false},
		{"single chunk", []nativePITRChunk{a, b, c}, nts(110, 0), nts(190, 0), []string{"a"}, false},
		{"overlapping duplicate takes furthest", []nativePITRChunk{a, chunk("a2", nts(100, 1), nts(250, 0)), b, c},
			nts(150, 0), nts(240, 0), []string{"a2"}, false},
		{"gap", []nativePITRChunk{a, c}, nts(150, 0), nts(350, 0), nil, true},
		{"from before first chunk", []nativePITRChunk{a, b, c}, nts(50, 0), nts(150, 0), nil, true},
		{"to after last chunk", []nativePITRChunk{a, b, c}, nts(150, 0), nts(500, 0), nil, true},
		{"no chunks", nil, nts(150, 0), nts(350, 0), nil, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := nativePITRChain(tc.chunks, tc.from, tc.to)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got chain %v", chainNames(got))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			names := chainNames(got)
			if len(names) != len(tc.want) {
				t.Fatalf("chain %v, want %v", names, tc.want)
			}
			for i := range names {
				if names[i] != tc.want[i] {
					t.Fatalf("chain %v, want %v", names, tc.want)
				}
			}
		})
	}
}

func entry(t *testing.T, at bson.Timestamp) bson.Raw {
	t.Helper()
	b, err := bson.Marshal(bson.D{{"ts", at}, {"op", "n"}, {"ns", ""}, {"o", bson.D{}}})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func batchTS(t *testing.T, l *nativePITRLoader) []bson.Timestamp {
	t.Helper()
	out := make([]bson.Timestamp, len(l.batch))
	for i, d := range l.batch {
		out[i].T, out[i].I, _ = d.Lookup("ts").TimestampOK()
	}
	return out
}

type sliceOplog []bson.Raw

func (s *sliceOplog) next() (bson.Raw, bool, error) {
	if len(*s) == 0 {
		return nil, false, nil
	}
	d := (*s)[0]
	*s = (*s)[1:]
	return d, true, nil
}

func op(t *testing.T, at bson.Timestamp, o string) bson.Raw {
	t.Helper()
	b, err := bson.Marshal(bson.D{{"ts", at}, {"t", int64(1)}, {"op", o}, {"ns", "db.c"}, {"o", bson.D{}}})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestNativePITRLoaderAdd(t *testing.T) {
	// snapshot oplog [bottom 100, top 200-5]; target 300-0
	newLoader := func(snap ...bson.Raw) *nativePITRLoader {
		s := sliceOplog(snap)
		return &nativePITRLoader{snap: &s, bottom: nts(100, 0), top: nts(200, 5), end: nts(300, 0)}
	}
	feed := func(t *testing.T, l *nativePITRLoader, docs ...bson.Raw) error {
		t.Helper()
		for _, d := range docs {
			done, err := l.add(d)
			if err != nil {
				return err
			}
			if done {
				break
			}
		}
		return l.finishOverlap()
	}
	expect := func(t *testing.T, l *nativePITRLoader, want ...bson.Timestamp) {
		t.Helper()
		got := batchTS(t, l)
		if len(got) != len(want) {
			t.Fatalf("loaded %v, want %v", got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("loaded %v, want %v", got, want)
			}
		}
	}

	t.Run("merges the overlap, loads (top, end], stops after end", func(t *testing.T) {
		l := newLoader(op(t, nts(150, 1), "i"), op(t, nts(200, 4), "u"), op(t, nts(200, 5), "i"))
		err := feed(t, l, op(t, nts(150, 1), "i"), op(t, nts(200, 4), "u"), op(t, nts(200, 5), "i"),
			op(t, nts(200, 6), "i"), op(t, nts(250, 1), "d"), op(t, nts(300, 0), "i"), op(t, nts(300, 1), "i"))
		if err != nil {
			t.Fatal(err)
		}
		expect(t, l, nts(200, 6), nts(250, 1), nts(300, 0))
		if l.last != nts(300, 0) || l.holes != 0 {
			t.Fatalf("last %v holes %d", l.last, l.holes)
		}
	})

	t.Run("noop top isn't in the chunks", func(t *testing.T) {
		l := newLoader(op(t, nts(190, 0), "i"), op(t, nts(200, 5), "n"))
		if err := feed(t, l, op(t, nts(190, 0), "i"), op(t, nts(210, 0), "i")); err != nil {
			t.Fatal(err)
		}
		expect(t, l, nts(210, 0))
	})

	t.Run("noops inside the overlap are skipped", func(t *testing.T) {
		l := newLoader(op(t, nts(150, 0), "n"), op(t, nts(190, 0), "i"), op(t, nts(195, 0), "n"), op(t, nts(200, 5), "i"))
		if err := feed(t, l, op(t, nts(190, 0), "i"), op(t, nts(200, 5), "i"), op(t, nts(210, 0), "i")); err != nil {
			t.Fatal(err)
		}
		expect(t, l, nts(210, 0))
	})

	t.Run("hole below the top is filled", func(t *testing.T) {
		l := newLoader(op(t, nts(190, 0), "i"), op(t, nts(200, 5), "i"))
		err := feed(t, l, op(t, nts(190, 0), "i"), op(t, nts(195, 0), "u"), op(t, nts(200, 5), "i"), op(t, nts(210, 0), "i"))
		if err != nil {
			t.Fatal(err)
		}
		expect(t, l, nts(195, 0), nts(210, 0))
		if l.holes != 1 {
			t.Fatalf("holes %d", l.holes)
		}
	})

	t.Run("snapshot write missing from the chunks fails before anything above the top is queued", func(t *testing.T) {
		l := newLoader(op(t, nts(190, 0), "i"), op(t, nts(195, 0), "i"), op(t, nts(200, 5), "n"))
		if err := feed(t, l, op(t, nts(190, 0), "i"), op(t, nts(210, 0), "i")); err == nil {
			t.Fatal("expected different-histories error")
		}
		expect(t, l)
	})

	t.Run("snapshot top write missing from the chunks fails", func(t *testing.T) {
		l := newLoader(op(t, nts(190, 0), "i"), op(t, nts(200, 5), "i"))
		if err := feed(t, l, op(t, nts(190, 0), "i"), op(t, nts(210, 0), "i")); err == nil {
			t.Fatal("expected different-histories error")
		}
		expect(t, l)
	})

	t.Run("same ts, different entry fails", func(t *testing.T) {
		l := newLoader(op(t, nts(190, 0), "i"), op(t, nts(200, 5), "i"))
		if err := feed(t, l, op(t, nts(190, 0), "d")); err == nil {
			t.Fatal("expected mismatch error")
		}
	})

	t.Run("same ts, op and ns but a different operation fails", func(t *testing.T) {
		withO := func(at bson.Timestamp, o, o2 bson.D) bson.Raw {
			d := bson.D{{"ts", at}, {"t", int64(1)}, {"op", "u"}, {"ns", "db.c"}, {"o", o}}
			if o2 != nil {
				d = append(d, bson.E{"o2", o2})
			}
			b, err := bson.Marshal(d)
			if err != nil {
				t.Fatal(err)
			}
			return b
		}
		set := func(v int) bson.D { return bson.D{{"$v", 2}, {"diff", bson.D{{"u", bson.D{{"x", v}}}}}} }
		id := func(v int) bson.D { return bson.D{{"_id", v}} }

		for name, c := range map[string][2]bson.Raw{
			"o":  {withO(nts(190, 0), set(1), id(1)), withO(nts(190, 0), set(2), id(1))},
			"o2": {withO(nts(190, 0), set(1), id(1)), withO(nts(190, 0), set(1), id(2))},
		} {
			l := newLoader(c[0], op(t, nts(200, 5), "i"))
			err := feed(t, l, c[1], op(t, nts(200, 5), "i"))
			if err == nil || !strings.Contains(err.Error(), "differs between the snapshot oplog and the chunks") {
				t.Errorf("%s differs: expected mismatch error, got %v", name, err)
			}
		}

		l := newLoader(withO(nts(190, 0), set(1), id(1)), op(t, nts(200, 5), "i"))
		if err := feed(t, l, withO(nts(190, 0), set(1), id(1)), op(t, nts(200, 5), "i")); err != nil {
			t.Errorf("identical entry: %v", err)
		}
	})

	t.Run("entries below the snapshot oplog bottom are skipped", func(t *testing.T) {
		l := newLoader(op(t, nts(100, 0), "i"), op(t, nts(200, 5), "i"))
		err := feed(t, l, op(t, nts(90, 0), "i"), op(t, nts(99, 9), "i"), op(t, nts(100, 0), "i"),
			op(t, nts(200, 5), "i"), op(t, nts(210, 0), "i"))
		if err != nil {
			t.Fatal(err)
		}
		expect(t, l, nts(210, 0))
	})

	t.Run("duplicate boundary entries from adjacent chunks are skipped", func(t *testing.T) {
		l := newLoader(op(t, nts(200, 5), "i"))
		// chunk 1
		if err := feed(t, l, op(t, nts(200, 5), "i"), op(t, nts(220, 0), "i"), op(t, nts(240, 0), "i")); err != nil {
			t.Fatal(err)
		}
		// chunk 2 starts before the end of chunk 1
		l.inChunk = false
		if err := feed(t, l, op(t, nts(220, 0), "i"), op(t, nts(240, 0), "i"), op(t, nts(260, 0), "i")); err != nil {
			t.Fatal(err)
		}
		expect(t, l, nts(220, 0), nts(240, 0), nts(260, 0))
	})

	t.Run("timestamps going backwards inside a chunk fail", func(t *testing.T) {
		l := newLoader(op(t, nts(200, 5), "i"))
		err := feed(t, l, op(t, nts(200, 5), "i"), op(t, nts(240, 0), "i"), op(t, nts(220, 0), "i"))
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("entries before the merged overlap are skipped", func(t *testing.T) {
		l := newLoader(op(t, nts(180, 0), "i"), op(t, nts(200, 5), "i"))
		l.from = nts(180, 0)
		// 150 is in the snapshot oplog but before the overlap: not merged, not a hole
		err := feed(t, l, op(t, nts(150, 0), "i"), op(t, nts(180, 0), "i"), op(t, nts(200, 5), "i"), op(t, nts(210, 0), "i"))
		if err != nil {
			t.Fatal(err)
		}
		expect(t, l, nts(210, 0))
	})

	t.Run("too many holes fail", func(t *testing.T) {
		l := newLoader(op(t, nts(200, 5), "i"))
		var err error
		for i := uint32(0); i <= nativePITRMaxHoles && err == nil; i++ {
			_, err = l.add(op(t, nts(150, i+1), "i"))
		}
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("entry without ts", func(t *testing.T) {
		l := newLoader()
		b, _ := bson.Marshal(bson.D{{"op", "n"}})
		if _, err := l.add(b); err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestNativePITRCoverage(t *testing.T) {
	a := chunk("a", nts(100, 1), nts(200, 5))
	b := chunk("b", nts(200, 5), nts(300, 2))
	c := chunk("c", nts(300, 2), nts(400, 9))
	old := chunk("old", nts(10, 0), nts(50, 0)) // separate earlier segment

	from, ov := nativePITRCoverage([]nativePITRChunk{c, old, b, a}, nts(350, 0))
	if from != nts(100, 1) || len(ov) != 0 {
		t.Fatalf("from %v overlaps %v", from, ov)
	}

	from, _ = nativePITRCoverage([]nativePITRChunk{a, c}, nts(350, 0))
	if from != nts(300, 2) {
		t.Fatalf("gap: from %v, want start of c", from)
	}

	from, _ = nativePITRCoverage([]nativePITRChunk{a, b}, nts(500, 0))
	if from != nts(500, 0) {
		t.Fatalf("not covered: from %v, want target", from)
	}

	x := chunk("x", nts(150, 0), nts(250, 0)) // overlaps a and b beyond the boundary
	_, ov = nativePITRCoverage([]nativePITRChunk{a, b, c, x}, nts(350, 0))
	if len(ov) == 0 {
		t.Fatal("expected overlap warning")
	}
}

func TestNativePITRParseChunkNames(t *testing.T) {
	files := []storage.FileInfo{
		{Name: "20260930/20260930090934-5181.20260930091934-5170.oplog.s2"},
		{Name: "20260930/20260930091934-5170.20260930092934-3.oplog.gz"},
		{Name: "20260930/20260930092934-3.20260930093934-1.oplog"},
		{Name: "20260930/garbage.txt"},
	}
	got := parseNativePITRChunks("rs01", files, log.DiscardEvent)
	want := []nativePITRChunk{
		{
			fname: "pbmPitr/rs01/20260930/20260930090934-5181.20260930091934-5170.oplog.s2",
			start: nts(1790759374, 5181), end: nts(1790759974, 5170), comp: compress.CompressionTypeS2,
		},
		{
			fname: "pbmPitr/rs01/20260930/20260930091934-5170.20260930092934-3.oplog.gz",
			start: nts(1790759974, 5170), end: nts(1790760574, 3), comp: compress.CompressionTypeGZIP,
		},
		{
			fname: "pbmPitr/rs01/20260930/20260930092934-3.20260930093934-1.oplog",
			start: nts(1790760574, 3), end: nts(1790761174, 1), comp: compress.CompressionTypeNone,
		},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d chunks, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("chunk %d:\n got %+v\nwant %+v", i, got[i], want[i])
		}
	}
}

// A chunk written the way the slicer writes it must read back through
// loadChunk's decode path with every ts intact.
func TestNativePITRChunkRoundTrip(t *testing.T) {
	dir := t.TempDir()
	stg, err := fs.New(&fs.Config{Path: dir})
	if err != nil {
		t.Fatal(err)
	}

	start, end := nts(1790758174, 5181), nts(1790758174, 5190)
	name := oplog.FormatChunkFilepath("rs01", start, end, compress.CompressionTypeS2)

	var buf bytes.Buffer
	w, err := compress.Compress(&buf, compress.CompressionTypeS2, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := start.I; i <= end.I; i++ {
		b, _ := bson.Marshal(bson.D{{"ts", nts(start.T, i)}, {"op", "n"}, {"o", bson.D{{"msg", "x"}}}})
		if _, err := w.Write(b); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stg.Save(name, &buf); err != nil {
		t.Fatal(err)
	}

	files, err := stg.List("pbmPitr/rs01", "")
	if err != nil {
		t.Fatal(err)
	}
	chunks := parseNativePITRChunks("rs01", files, log.DiscardEvent)
	if len(chunks) != 1 || chunks[0].fname != name {
		t.Fatalf("chunks %+v, want %s", chunks, name)
	}

	sr, err := stg.SourceReader(chunks[0].fname)
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()
	rdr, err := compress.Decompress(sr, chunks[0].comp)
	if err != nil {
		t.Fatal(err)
	}
	src := db.NewBufferlessBSONSource(rdr)
	src.SetMaxBSONSize(nativePITRMaxEntrySize)
	n := 0
	for doc := src.LoadNext(); doc != nil; doc = src.LoadNext() {
		if _, _, ok := bson.Raw(doc).Lookup("ts").TimestampOK(); !ok {
			t.Fatalf("entry %d without ts", n)
		}
		n++
	}
	if src.Err() != nil {
		t.Fatal(src.Err())
	}
	if n != int(end.I-start.I+1) {
		t.Fatalf("read %d entries, want %d", n, end.I-start.I+1)
	}
}

func TestNativePITRChainOverlaps(t *testing.T) {
	a := chunk("a", nts(100, 1), nts(200, 5))
	b := chunk("b", nts(200, 5), nts(300, 2))
	x := chunk("x", nts(250, 0), nts(400, 0))
	if ov := nativePITRChainOverlaps([]nativePITRChunk{a, b}); len(ov) != 0 {
		t.Fatalf("shared boundary reported as overlap: %v", ov)
	}
	if ov := nativePITRChainOverlaps([]nativePITRChunk{a, b, x}); len(ov) != 1 {
		t.Fatalf("overlap not reported: %v", ov)
	}
}

func TestNativeBigDocsSupported(t *testing.T) {
	for _, c := range []struct {
		v    []int
		want bool
	}{
		{[]int{6, 0, 15}, false},
		{[]int{7, 0, 5}, false},
		{[]int{7, 0, 6}, true},
		{[]int{7, 0, 14}, true},
		{[]int{7, 3, 0}, false},
		{[]int{8, 0, 0}, true},
		{nil, false},
	} {
		if got := nativeBigDocsSupported(c.v); got != c.want {
			t.Errorf("%v: got %v, want %v", c.v, got, c.want)
		}
	}
}

// restore-finish after an agent restart rebuilds the restore from ext.dump:
// the native PITR mode must survive it, or the window is silently not loaded.
func TestNativePITRExtDumpRoundTrip(t *testing.T) {
	for _, native := range []bool{true, false} {
		t.Run(fmt.Sprintf("native=%v", native), func(t *testing.T) {
			dir := t.TempDir()
			stg, err := fs.New(&fs.Config{Path: dir})
			if err != nil {
				t.Fatal(err)
			}
			cfgPath := filepath.Join(t.TempDir(), "pbm.yaml")
			cfg := fmt.Sprintf("storage:\n  type: filesystem\n  filesystem:\n    path: %s\n", dir)
			if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
				t.Fatal(err)
			}

			const name, rs, node = "2026-01-01T00:00:00Z", "rs0", "node0:27017"
			r := &PhysRestore{
				stg:           stg,
				name:          name,
				rsConf:        &topo.RSConfig{ID: rs},
				nodeInfo:      &topo.NodeInfo{Me: node, SetName: rs},
				restoreTS:     nts(100, 1),
				syncPathNode:  fmt.Sprintf("%s/%s/rs.%s/node.%s", defs.PhysRestoresDir, name, rs, node),
				nativePITR:    native,
				nativeBigDocs: native,
			}
			if err := r.extDumpFromPhysRestore(&RestoreMeta{Name: name}); err != nil {
				t.Fatal(err)
			}

			l := log.New(nil, rs, node).NewEvent("restore", name, "", bson.Timestamp{})
			got, _, err := physRestoreFromExtDump(l, &ExtFinishCmd{
				RestoreName: name, CfgPath: cfgPath, RS: rs, Node: node,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer got.closeStorages()

			if got.nativePITR != native || got.nativeBigDocs != native {
				t.Errorf("nativePITR %v, nativeBigDocs %v after restore-finish; want %v",
					got.nativePITR, got.nativeBigDocs, native)
			}
			if got.restoreTS != r.restoreTS {
				t.Errorf("restoreTS %v, want %v", got.restoreTS, r.restoreTS)
			}
		})
	}
}

func TestNativePITRPlanChain(t *testing.T) {
	// snapshot oplog [bottom, top] = [{500 1}, {1000 7}], target {1300 0}
	top, bottom, end := nts(1000, 7), nts(500, 1), nts(1300, 0)
	early := chunk("early", nts(400, 1), nts(990, 3))
	mid := chunk("mid", nts(990, 3), nts(1200, 4))
	atTop := chunk("atTop", top, nts(1200, 4))
	late := chunk("late", nts(1200, 4), nts(1400, 2))

	cases := []struct {
		name     string
		chunks   []nativePITRChunk
		top      bson.Timestamp
		bottom   bson.Timestamp
		wantFrom bson.Timestamp
		want     []string
		wantErr  bool
	}{
		{"overlap starts 60s below the top", []nativePITRChunk{early, mid, late}, top, bottom,
			nts(940, 0), []string{"early", "mid", "late"}, false},
		{"overlap clamped to the snapshot bottom", []nativePITRChunk{early, mid, late}, top, nts(970, 2),
			nts(970, 2), []string{"early", "mid", "late"}, false},
		{"pitr started at the snapshot top", []nativePITRChunk{atTop, late}, top, bottom,
			top, []string{"atTop", "late"}, false},
		{"pitr started just below the top", []nativePITRChunk{mid, late}, top, bottom,
			nts(990, 3), []string{"mid", "late"}, false},
		{"chunks start after the top", []nativePITRChunk{chunk("after", nts(1000, 8), nts(1400, 2))}, top, bottom,
			bson.Timestamp{}, nil, true},
		{"gap between the chunks", []nativePITRChunk{early, late}, top, bottom,
			bson.Timestamp{}, nil, true},
		{"overlapping chunks in the chain", []nativePITRChunk{early, chunk("x", nts(980, 0), nts(1350, 0))}, top, bottom,
			bson.Timestamp{}, nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, err := nativePITRPlanChain(c.chunks, c.top, c.bottom, end)
			if c.wantErr {
				if err == nil {
					t.Fatalf("want an error, got chain %v from %v", chainNames(p.chain), p.from)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if p.from != c.wantFrom {
				t.Errorf("from %v, want %v", p.from, c.wantFrom)
			}
			if got := chainNames(p.chain); !slices.Equal(got, c.want) {
				t.Errorf("chain %v, want %v", got, c.want)
			}
		})
	}
}
