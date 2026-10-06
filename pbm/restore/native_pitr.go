package restore

// Native PITR for external restores.
//
// Instead of replaying the PITR window with applyOps after the restore
// (`pbm oplog-replay`), the window's oplog entries are appended to the
// restored node's own local.oplog.rs during prepareData (a standalone mongod,
// after the datadir cleanup). recoverStandaloneFromOplog then replays them with
// mongod's native oplog applier (startup recovery) up to restoreTS, the same
// way it already replays the snapshot's own oplog tail.
//
// PITR chunks are raw copies of oplog.rs entries (see oplog.OplogBackup), so
// the entries are inserted unmodified. The server prepends an _id to each one,
// which OplogEntryBase allows.

import (
	"context"
	"io"
	"path"
	"sort"
	"time"

	"github.com/mongodb/mongo-tools/common/db"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/util"
)

const (
	nativePITRBatchDocs  = 1000
	nativePITRBatchBytes = 8 << 20
	// oplog entries may exceed the 16MB user document limit by the
	// internal BSON overhead (BSONObjMaxInternalSize)
	nativePITRMaxEntrySize = db.MaxBSONSize + 16*1024
	nativePITRProgressFreq = 30 * time.Second
	// how far below the snapshot's top the chunks are merged with its oplog
	nativePITROverlap = 60 * time.Second
	// oplog holes are entries in flight at the snapshot: a few at most
	nativePITRMaxHoles = 10000
)

type nativePITRChunk struct {
	fname string // full path on the storage
	start bson.Timestamp
	end   bson.Timestamp
	comp  compress.CompressionType
}

// nativePITRChain picks a contiguous chain of chunks covering [from, to].
// Chunks may overlap (adjacent chunks share their boundary entry). Starting at
// `from`, it repeatedly takes the chunk that starts at or before the covered
// point and reaches furthest, failing on any gap.
func nativePITRChain(chunks []nativePITRChunk, from, to bson.Timestamp) ([]nativePITRChunk, error) {
	sorted := make([]nativePITRChunk, len(chunks))
	copy(sorted, chunks)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].start.Compare(sorted[j].start) == -1
	})

	var chain []nativePITRChunk
	cur := from
	for i := 0; ; {
		best := -1
		for ; i < len(sorted) && sorted[i].start.Compare(cur) <= 0; i++ {
			if best == -1 || sorted[i].end.Compare(sorted[best].end) == 1 {
				best = i
			}
		}
		if best == -1 || sorted[best].end.Compare(cur) == -1 ||
			(len(chain) > 0 && sorted[best].end.Compare(cur) == 0) {
			return nil, errors.Errorf("no PITR chunk covers %v (target %v)", cur, to)
		}
		chain = append(chain, sorted[best])
		cur = sorted[best].end
		if cur.Compare(to) >= 0 {
			return chain, nil
		}
	}
}

// nativePITRChunks lists the replset's PITR chunks on the storage.
func (r *PhysRestore) nativePITRChunks() ([]nativePITRChunk, error) {
	rs := util.MakeReverseRSMapFunc(r.rsMap)(r.nodeInfo.SetName)
	prefix := path.Join(defs.PITRfsPrefix, rs)

	files, err := r.stg.List(prefix, "")
	if err != nil {
		return nil, errors.Wrapf(err, "list %s", prefix)
	}

	return parseNativePITRChunks(rs, files, r.log), nil
}

// parseNativePITRChunks parses PITR chunk names listed under pbmPitr/<rs>
// (<day>/<start>.<end>.oplog[.<compression>]).
func parseNativePITRChunks(rs string, files []storage.FileInfo, l log.LogEvent) []nativePITRChunk {
	chunks := make([]nativePITRChunk, 0, len(files))
	for _, f := range files {
		m := oplog.MakeChunkMetaFromFilepath(path.Join(rs, f.Name))
		if m == nil {
			l.Debug("native pitr: skip %s/%s/%s: not a PITR chunk", defs.PITRfsPrefix, rs, f.Name)
			continue
		}
		chunks = append(chunks, nativePITRChunk{
			fname: m.FName,
			start: m.StartTS,
			end:   m.EndTS,
			comp:  m.Compression,
		})
	}

	return chunks
}

// nativePITRPreflight runs while the cluster is still up: it fails the restore
// before the datadir is touched if the storage has no chunk chain reaching
// restoreTS, and logs how far back the chain goes (the snapshot's oplog must
// end within that range).
func (r *PhysRestore) nativePITRPreflight() error {
	chunks, err := r.nativePITRChunks()
	if err != nil {
		return err
	}
	if len(chunks) == 0 {
		return errors.New("native pitr: no PITR chunks found on the storage")
	}

	from, overlaps := nativePITRCoverage(chunks, r.restoreTS)
	if _, err := nativePITRChain(chunks, from, r.restoreTS); err != nil {
		return errors.Wrap(err, "native pitr")
	}

	// the coverage can reach back well before the snapshot: overlaps there
	// may not matter. The chain actually loaded is checked in planNativePITR.
	if len(overlaps) > 0 {
		r.log.Warning("native pitr: overlapping PITR chunks %s and %s (%d pair(s)): "+
			"the restore fails if they are in the range it loads; "+
			"make sure all chunks under this prefix come from the source cluster",
			overlaps[0][0], overlaps[0][1], len(overlaps))
	}
	r.log.Info("native pitr: %d chunk(s) on the storage, contiguous coverage %v - %v; "+
		"the snapshot's oplog must end within that range", len(chunks), from, r.restoreTS)
	return nil
}

// nativePITRCoverage returns the start of the contiguous chunk coverage that
// reaches `to` (or `to` itself if no chunk covers it) in a single sorted pass,
// and the pairs of chunks in that range that overlap beyond a shared boundary.
func nativePITRCoverage(chunks []nativePITRChunk, to bson.Timestamp) (bson.Timestamp, [][2]string) {
	sorted := make([]nativePITRChunk, len(chunks))
	copy(sorted, chunks)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].start.Compare(sorted[j].start) == -1
	})

	from := to
	var segStart, segEnd bson.Timestamp
	var seg, prev []nativePITRChunk
	var overlaps [][2]string
	for i, c := range sorted {
		if i == 0 || c.start.Compare(segEnd) == 1 {
			segStart, segEnd, seg = c.start, c.end, nil
		} else if c.end.Compare(segEnd) == 1 {
			segEnd = c.end
		}
		seg = append(seg, c)
		if segStart.Compare(to) <= 0 && segEnd.Compare(to) >= 0 {
			from, prev = segStart, seg
		}
	}

	for i := 1; i < len(prev); i++ {
		if prev[i].start.Compare(prev[i-1].end) == -1 {
			overlaps = append(overlaps, [2]string{prev[i-1].fname, prev[i].fname})
		}
	}

	return from, overlaps
}

// nativePITRPlan is what loadNativePITR loads: chain covers [from, end],
// and [from, top] is merged with the snapshot's oplog.
type nativePITRPlan struct {
	top, bottom bson.Timestamp // snapshot's oplog
	from        bson.Timestamp // start of the merged overlap
	chain       []nativePITRChunk
}

// planNativePITR checks the PITR chunks against the copied snapshot's oplog.
// It runs in prepareData before local.* is modified, so if the chunks don't
// connect to the snapshot the restore fails before the snapshot's replica set
// metadata is changed.
// It returns nil if the snapshot's oplog already reaches restoreTS.
func (r *PhysRestore) planNativePITR(ctx context.Context, c *mongo.Client) (*nativePITRPlan, error) {
	oplogColl := c.Database("local").Collection("oplog.rs")
	topRaw, top, err := oplogEdgeEntry(ctx, oplogColl, -1)
	if err != nil {
		return nil, err
	}
	_, bottom, err := oplogEdgeEntry(ctx, oplogColl, 1)
	if err != nil {
		return nil, err
	}
	if top.Compare(r.restoreTS) >= 0 {
		r.log.Info("native pitr: oplog top %v already reaches %v, nothing to load", top, r.restoreTS)
		return nil, nil
	}

	chunks, err := r.nativePITRChunks()
	if err != nil {
		return nil, err
	}
	p, err := nativePITRPlanChain(chunks, top, bottom, r.restoreTS)
	if err != nil {
		return nil, errors.Wrap(err, "native pitr")
	}
	r.log.Info("native pitr: loading oplog (%v, %v] from %d chunk(s); merging the overlap "+
		"[%v, %v] with the snapshot oplog (bottom %v, top op %q)",
		top, r.restoreTS, len(p.chain), p.from, top, bottom, topRaw.Lookup("op").StringValue())
	return p, nil
}

// nativePITRPlanChain picks the chunks to load for a snapshot oplog [bottom,
// top] and a target `end`. The overlap merged with the snapshot oplog starts
// nativePITROverlap below its top, no earlier than its bottom, and no earlier
// than where the chunk coverage reaching `end` starts: PITR that began right
// after the backup has chunks only from about the snapshot's top.
func nativePITRPlanChain(chunks []nativePITRChunk, top, bottom, end bson.Timestamp) (*nativePITRPlan, error) {
	covFrom, _ := nativePITRCoverage(chunks, end)
	if covFrom.Compare(top) == 1 {
		return nil, errors.Errorf("PITR chunks reaching %v start at %v, after the snapshot oplog top %v: "+
			"the chunks don't cover the snapshot", end, covFrom, top)
	}

	from := bson.Timestamp{T: top.T - uint32(nativePITROverlap/time.Second)}
	if from.Compare(bottom) == -1 {
		from = bottom
	}
	if from.Compare(covFrom) == -1 {
		from = covFrom
	}

	chain, err := nativePITRChain(chunks, from, end)
	if err != nil {
		return nil, errors.Wrap(err, "snapshot oplog top is not covered by PITR chunks")
	}
	if ov := nativePITRChainOverlaps(chain); len(ov) > 0 {
		return nil, errors.Errorf("overlapping PITR chunks %s and %s", ov[0][0], ov[0][1])
	}

	return &nativePITRPlan{top: top, bottom: bottom, from: from, chain: chain}, nil
}

// oplogEdgeEntry returns the oplog's last (dir -1) or first (dir 1) entry.
func oplogEdgeEntry(ctx context.Context, oplogColl *mongo.Collection, dir int) (bson.Raw, bson.Timestamp, error) {
	var ts bson.Timestamp
	raw, err := oplogColl.FindOne(ctx, bson.D{},
		options.FindOne().SetSort(bson.D{{"$natural", dir}})).Raw()
	if err != nil {
		return nil, ts, errors.Wrap(err, "get the edge of the oplog")
	}
	var ok bool
	ts.T, ts.I, ok = raw.Lookup("ts").TimestampOK()
	if !ok {
		return nil, ts, errors.Errorf("get the timestamp of record %v", raw)
	}
	return raw, ts, nil
}

// loadNativePITR appends oplog entries from the snapshot's oplog top up to and
// including restoreTS into local.oplog.rs. Must run on a standalone mongod.
//
// The chunks overlap the snapshot's own oplog from nativePITROverlap below its
// top. That overlap is merged against the snapshot's oplog rather than
// skipped: the slicer means to drop noops (so the snapshot's top may not be in
// the chunks), and a crash-consistent snapshot may miss in-flight entries just
// below its top (oplog holes) that a running node would truncate via its
// oplogTruncateAfterPoint - which prepareData overrides with restoreTS.
// Missing entries are inserted, matching entries are skipped, and any other
// difference fails the restore.
//
// Forward reads of the oplog block on a standalone once entries above the
// snapshot's top are inserted, so the merge cursor is drained before the
// first insert and only reverse reads are used afterwards.
//
// The plan comes from planNativePITR, run before prepareData modifies local.*.
func (r *PhysRestore) loadNativePITR(ctx context.Context, c *mongo.Client, p *nativePITRPlan) error {
	if p == nil {
		return nil
	}
	oplogColl := c.Database("local").Collection("oplog.rs")
	from, top, bottom := p.from, p.top, p.bottom

	cur, err := oplogColl.Find(ctx,
		bson.D{{"ts", bson.D{{"$gte", from}, {"$lte", top}}}},
		options.Find().SetSort(bson.D{{"$natural", 1}}))
	if err != nil {
		return errors.Wrap(err, "native pitr: open snapshot oplog cursor")
	}
	defer cur.Close(ctx)

	l := &nativePITRLoader{
		ctx:    ctx,
		coll:   oplogColl,
		snap:   &cursorOplog{ctx: ctx, cur: cur},
		from:   from,
		bottom: bottom,
		top:    top,
		end:    r.restoreTS,
		start:  time.Now(),
		logf:   r.log.Info,
	}
	l.lastLog = l.start

	for _, ch := range p.chain {
		done, err := l.loadChunk(r.stg, ch, func(msg string, args ...any) {
			r.log.Info(msg, args...)
		})
		if err != nil {
			return errors.Wrapf(err, "native pitr: chunk %s", ch.fname)
		}
		if done {
			break
		}
	}
	if err := l.finishOverlap(); err != nil {
		return errors.Wrap(err, "native pitr")
	}
	if err := l.flush(); err != nil {
		return errors.Wrap(err, "native pitr")
	}

	_, newTop, err := oplogEdgeEntry(ctx, oplogColl, -1)
	if err != nil {
		return err
	}
	want := top
	if l.last.Compare(want) == 1 {
		want = l.last
	}
	if newTop.Compare(want) != 0 {
		return errors.Errorf("native pitr: oplog top after load is %v, expected %v", newTop, want)
	}
	if l.last.Compare(r.restoreTS) == -1 {
		r.log.Warning("native pitr: the last loaded entry %v is before the target %v "+
			"(no writes in between, or the chunks end early)", l.last, r.restoreTS)
	}

	r.log.Info("native pitr: loaded %d oplog entries (%d bytes, %d hole(s) filled below the snapshot top) "+
		"in %v, oplog %v -> %v",
		l.inserted, l.bytes, l.holes, time.Since(l.start).Round(time.Second), top, newTop)
	return nil
}

// snapshotOplog iterates the snapshot's oplog entries in the overlap with the
// chunks, in ts order.
type snapshotOplog interface {
	next() (bson.Raw, bool, error)
}

type cursorOplog struct {
	ctx context.Context
	cur *mongo.Cursor
}

func (c *cursorOplog) next() (bson.Raw, bool, error) {
	if c.cur.Next(c.ctx) {
		return c.cur.Current, true, nil
	}
	return nil, false, c.cur.Err()
}

type nativePITRLoader struct {
	ctx  context.Context
	coll *mongo.Collection
	snap snapshotOplog

	from   bson.Timestamp // start of the merged overlap
	bottom bson.Timestamp // snapshot's oplog bottom
	top    bson.Timestamp // snapshot's oplog top
	end    bson.Timestamp // restoreTS, inclusive
	seen   bson.Timestamp // last chunk entry processed (dedups chunk boundaries)
	last   bson.Timestamp // last entry above top queued for insert

	// the snapshot entry the overlap merge is at
	snapCur  bson.Raw
	snapTS   bson.Timestamp
	snapDone bool
	snapInit bool

	overlapDone bool

	batch      []bson.Raw
	batchBytes int
	inserted   int64
	bytes      int64
	holes      int64

	start   time.Time
	lastLog time.Time
	logf    func(string, ...any)

	inChunk bool // an entry above seen was read from the current chunk
}

// loadChunk streams one chunk into the oplog. It returns true once an entry
// beyond the target is reached.
func (l *nativePITRLoader) loadChunk(
	stg storage.Storage,
	ch nativePITRChunk,
	logf func(string, ...any),
) (bool, error) {
	sr, err := stg.SourceReader(ch.fname)
	if err != nil {
		return false, errors.Wrap(err, "get object from the storage")
	}
	defer sr.Close()

	rdr, err := compress.Decompress(sr, ch.comp)
	if err != nil {
		return false, errors.Wrap(err, "decompress object")
	}
	defer rdr.Close()

	src := db.NewBufferlessBSONSource(rdr)
	src.SetMaxBSONSize(nativePITRMaxEntrySize)

	l.inChunk = false
	for {
		doc := src.LoadNext()
		if doc == nil {
			return false, errors.Wrap(src.Err(), "read oplog entry")
		}

		done, err := l.add(bson.Raw(doc))
		if err != nil {
			return false, err
		}
		if done {
			// Read the object to the end: the storage downloader's workers
			// block forever on an abandoned reader, holding the download
			// buffer every later read on this storage waits for.
			_, err = io.Copy(io.Discard, sr)
			return true, errors.Wrap(err, "drain the rest of the chunk")
		}

		if time.Since(l.lastLog) >= nativePITRProgressFreq {
			l.lastLog = time.Now()
			logf("native pitr: loaded %d entries (%d bytes), at %v of %v",
				l.inserted, l.bytes, l.seen, l.end)
		}
	}
}

// add handles one entry read from the chunks: entries up to the snapshot's top
// are merged with the snapshot oplog, entries in (top, end] are queued.
func (l *nativePITRLoader) add(doc bson.Raw) (bool, error) {
	var ts bson.Timestamp
	var ok bool
	ts.T, ts.I, ok = doc.Lookup("ts").TimestampOK()
	if !ok {
		return false, errors.New("oplog entry without ts")
	}

	if !l.seen.IsZero() && ts.Compare(l.seen) <= 0 {
		// the head of a chunk overlaps what the previous one covered
		if !l.inChunk {
			return false, nil
		}
		return false, errors.Errorf("oplog entry %v after %v: timestamps go backwards", ts, l.seen)
	}
	l.seen = ts
	l.inChunk = true

	if ts.Compare(l.end) == 1 {
		return true, nil
	}

	if ts.Compare(l.from) == -1 || ts.Compare(l.bottom) == -1 {
		// before the merged overlap: already in the data
		return false, nil
	}
	if ts.Compare(l.top) <= 0 {
		present, err := l.inSnapshot(ts, doc)
		if err != nil || present {
			return false, err
		}
		l.holes++
		if l.holes > nativePITRMaxHoles {
			return false, errors.Errorf("more than %d chunk entries below the snapshot top are missing "+
				"from the snapshot oplog: not a few in-flight holes", nativePITRMaxHoles)
		}
		if l.logf != nil {
			l.logf("native pitr: filling oplog hole %v (op %q, ns %q)",
				ts, doc.Lookup("op").StringValue(), doc.Lookup("ns").StringValue())
		}
	} else {
		if !l.overlapDone {
			// fail before loading anything above the top if the overlap differs
			if err := l.finishOverlap(); err != nil {
				return false, err
			}
		}
		l.last = ts
	}

	// holes are held until the overlap merge is done: inserted earlier, the
	// merge cursor could read them back as snapshot entries
	if l.overlapDone && len(l.batch) > 0 &&
		(len(l.batch) >= nativePITRBatchDocs || l.batchBytes+len(doc) > nativePITRBatchBytes) {
		if err := l.flush(); err != nil {
			return false, err
		}
	}
	l.batch = append(l.batch, doc)
	l.batchBytes += len(doc)

	return false, nil
}

// inSnapshot advances the snapshot oplog to ts and reports whether the
// chunk entry is already there. Snapshot entries the chunks skip over must
// be noops (the slicer doesn't save them); anything else means the snapshot
// and the chunks come from different histories.
func (l *nativePITRLoader) inSnapshot(ts bson.Timestamp, doc bson.Raw) (bool, error) {
	for {
		if err := l.advanceSnapshot(); err != nil {
			return false, err
		}
		if l.snapDone {
			return false, nil
		}
		switch l.snapTS.Compare(ts) {
		case 1:
			return false, nil
		case 0:
			if err := sameEntry(l.snapCur, doc); err != nil {
				return false, errors.Wrapf(err, "entry %v differs between the snapshot oplog and the chunks", ts)
			}
			l.snapInit = false // consume
			return true, nil
		}
		if err := l.skipSnapshotEntry(); err != nil {
			return false, err
		}
	}
}

// finishOverlap checks the snapshot entries the chunks never reached.
func (l *nativePITRLoader) finishOverlap() error {
	l.overlapDone = true
	for {
		if err := l.advanceSnapshot(); err != nil {
			return err
		}
		if l.snapDone {
			return nil
		}
		if err := l.skipSnapshotEntry(); err != nil {
			return err
		}
	}
}

func (l *nativePITRLoader) advanceSnapshot() error {
	if l.snapInit || l.snapDone {
		return nil
	}
	raw, ok, err := l.snap.next()
	if err != nil {
		return errors.Wrap(err, "read the snapshot oplog")
	}
	if !ok {
		l.snapDone = true
		return nil
	}
	var good bool
	l.snapTS.T, l.snapTS.I, good = raw.Lookup("ts").TimestampOK()
	if !good {
		return errors.Errorf("snapshot oplog entry without ts: %v", raw)
	}
	// the cursor buffer is reused on the next call
	l.snapCur = append(l.snapCur[:0], raw...)
	l.snapInit = true
	return nil
}

func (l *nativePITRLoader) skipSnapshotEntry() error {
	if op := l.snapCur.Lookup("op").StringValue(); op != string(defs.OperationNoop) {
		return errors.Errorf("snapshot oplog entry %v (op %q) is not in the PITR chunks: "+
			"the snapshot and the chunks come from different histories", l.snapTS, op)
	}
	l.snapInit = false
	return nil
}

// sameEntry compares the identifying fields of two copies of an oplog entry.
func sameEntry(a, b bson.Raw) error {
	for _, k := range []string{"t", "op", "ns"} {
		if !a.Lookup(k).Equal(b.Lookup(k)) {
			return errors.Errorf("field %q: %v vs %v", k, a.Lookup(k), b.Lookup(k))
		}
	}
	return nil
}

// flush inserts the queued entries.
func (l *nativePITRLoader) flush() error {
	// the batch may exceed the limits when holes were held for the merge
	for i := 0; i < len(l.batch); {
		j, size := i, 0
		for j < len(l.batch) && j-i < nativePITRBatchDocs &&
			(j == i || size+len(l.batch[j]) <= nativePITRBatchBytes) {
			size += len(l.batch[j])
			j++
		}
		if err := l.insert(l.batch[i:j]); err != nil {
			return err
		}
		l.inserted += int64(j - i)
		l.bytes += int64(size)
		i = j
	}

	l.batch = l.batch[:0]
	l.batchBytes = 0
	return nil
}

// insert uses a raw insert command because the driver's InsertMany would add
// an _id client-side.
func (l *nativePITRLoader) insert(docs []bson.Raw) error {
	res := l.coll.Database().RunCommand(l.ctx, bson.D{
		{"insert", l.coll.Name()},
		{"documents", docs},
		{"ordered", true},
	})
	var reply struct {
		OK          float64       `bson:"ok"`
		N           int           `bson:"n"`
		WriteErrors bson.RawArray `bson:"writeErrors"`
	}
	if err := res.Decode(&reply); err != nil {
		return errors.Wrap(err, "insert oplog entries")
	}
	if len(reply.WriteErrors) > 0 || reply.N != len(docs) {
		return errors.Errorf("insert oplog entries: inserted %d of %d, write errors: %v",
			reply.N, len(docs), reply.WriteErrors)
	}
	return nil
}

// nativeBigDocsSupported reports whether mongod has the
// allowDocumentsGreaterThanMaxUserSize parameter (7.0.6+, 8.0+).
func nativeBigDocsSupported(v []int) bool {
	at := func(i int) int {
		if i < len(v) {
			return v[i]
		}
		return 0
	}
	switch {
	case at(0) >= 8:
		return true
	case at(0) == 7 && at(1) == 0:
		return at(2) >= 6
	default:
		return false
	}
}

// nativePITRChainOverlaps returns the consecutive chunks of a chain that
// overlap beyond their shared boundary entry.
func nativePITRChainOverlaps(chain []nativePITRChunk) [][2]string {
	var ov [][2]string
	for i := 1; i < len(chain); i++ {
		if chain[i].start.Compare(chain[i-1].end) == -1 {
			ov = append(ov, [2]string{chain[i-1].fname, chain[i].fname})
		}
	}
	return ov
}
