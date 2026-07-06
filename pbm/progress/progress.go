package progress

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

const (
	mb                            = 1024 * 1024
	DefaultProgressThresholdBytes = 16 << 20
)

// Progress captures an operation progress snapshot. Byte counters represent
// bytes transferred to/from backup storage.
type Progress struct {
	StartedAt  int64 `bson:"started_at" json:"started_at"`
	UpdatedAt  int64 `bson:"updated_at" json:"updated_at"`
	DoneBytes  int64 `bson:"done_bytes,omitempty" json:"done_bytes,omitempty"`
	TotalBytes int64 `bson:"total_bytes,omitempty" json:"total_bytes,omitempty"`
	DoneItems  int64 `bson:"done_items,omitempty" json:"done_items,omitempty"`
	TotalItems int64 `bson:"total_items,omitempty" json:"total_items,omitempty"`
	// ThroughputBytesPerSecond is the last known completed transfer rate.
	ThroughputBytesPerSecond int64 `bson:"throughput_bps,omitempty" json:"throughput_bps,omitempty"`
}

func New(totalBytes, totalItems int64) Progress {
	now := time.Now().Unix()
	return Progress{StartedAt: now, UpdatedAt: now, TotalBytes: totalBytes, TotalItems: totalItems}
}

func (p Progress) Percent() (float64, bool) {
	switch {
	case p.TotalBytes > 0:
		return percent(p.DoneBytes, p.TotalBytes), true
	case p.TotalItems > 0:
		return percent(p.DoneItems, p.TotalItems), true
	default:
		return 0, false
	}
}

func (p Progress) ThroughputMBps(now int64) float64 {
	_ = now
	if p.ThroughputBytesPerSecond > 0 {
		return float64(p.ThroughputBytesPerSecond) / float64(mb)
	}

	return 0
}

func (p Progress) ETA() (time.Duration, bool) {
	if p.TotalBytes <= 0 || p.DoneBytes <= 0 || p.DoneBytes >= p.TotalBytes || p.ThroughputBytesPerSecond <= 0 {
		return 0, false
	}

	return time.Duration((p.TotalBytes-p.DoneBytes)/p.ThroughputBytesPerSecond) * time.Second, true
}

func (p Progress) Elapsed(now int64) time.Duration {
	if p.StartedAt <= 0 || now <= p.StartedAt {
		return 0
	}
	return time.Duration(now-p.StartedAt) * time.Second
}

func (p Progress) StringAt(now int64) string {
	parts := []string{fmt.Sprintf("elapsed=%s", FormatDuration(p.Elapsed(now)))}
	if p.TotalItems > 0 {
		parts = append(parts, fmt.Sprintf("items=%d/%d", p.DoneItems, p.TotalItems))
	}
	if p.DoneBytes > 0 || p.TotalBytes > 0 {
		b := storage.PrettySize(p.DoneBytes)
		if p.TotalBytes > 0 {
			b += "/" + storage.PrettySize(p.TotalBytes)
		}
		parts = append(parts, "transferred="+b)
	}
	if pct, ok := p.Percent(); ok {
		parts = append(parts, fmt.Sprintf("done=%.1f%%", pct))
	}
	if mbps := p.ThroughputMBps(now); mbps > 0 {
		parts = append(parts, fmt.Sprintf("throughput=%.2fMB/s", mbps))
	}
	if eta, ok := p.ETA(); ok {
		parts = append(parts, "eta="+FormatDuration(eta))
	}

	return strings.Join(parts, ", ")
}

func FormatDuration(d time.Duration) string {
	if d < 0 {
		return "-"
	}
	d = d.Truncate(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	if h > 0 {
		return fmt.Sprintf("%dh%02dm%02ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm%02ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

func percent(done, total int64) float64 {
	if total <= 0 {
		return 0
	}
	p := float64(done) * 100 / float64(total)
	if p > 100 {
		return 100
	}
	return p
}

type Logger interface {
	Info(msg string, args ...any)
	Warning(msg string, args ...any)
}

// Reporter periodically persists and logs operation progress.
type Reporter struct {
	ctx    context.Context
	log    Logger
	update func(context.Context, Progress) error

	startedAt int64
	totalB    atomic.Int64
	doneB     atomic.Int64
	totalI    atomic.Int64
	doneI     atomic.Int64
	lastTick  atomic.Int64
	lastBytes atomic.Int64
	lastBPS   atomic.Int64
	stop      chan struct{}
}

func NewReporter(
	ctx context.Context,
	log Logger,
	interval time.Duration,
	totalBytes int64,
	totalItems int64,
	update func(context.Context, Progress) error,
) *Reporter {
	r := &Reporter{ctx: ctx, log: log, update: update, startedAt: time.Now().Unix(), stop: make(chan struct{})}
	r.totalB.Store(totalBytes)
	r.totalI.Store(totalItems)
	r.lastTick.Store(r.startedAt)
	_ = r.Flush()

	go func() {
		tk := time.NewTicker(interval)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-r.stop:
				return
			case <-tk.C:
				p := r.TickSnapshot()
				if err := r.FlushProgress(p); err != nil && log != nil {
					log.Warning("update progress: %v", err)
				}
				if log != nil {
					log.Info("progress: %s", p.StringAt(time.Now().Unix()))
				}
			}
		}
	}()

	return r
}

func (r *Reporter) AddBytes(n int64) {
	if n == 0 {
		return
	}

	for {
		old := r.doneB.Load()
		next := old + n
		if next < 0 {
			next = 0
		}
		if r.doneB.CompareAndSwap(old, next) {
			return
		}
	}
}

func (r *Reporter) AddItems(n int64) {
	if n > 0 {
		r.doneI.Add(n)
	}
}

func (r *Reporter) SetTotalBytes(n int64) { r.totalB.Store(n) }
func (r *Reporter) SetTotalItems(n int64) { r.totalI.Store(n) }

func (r *Reporter) Snapshot() Progress {
	return Progress{
		StartedAt:                r.startedAt,
		UpdatedAt:                time.Now().Unix(),
		DoneBytes:                r.doneB.Load(),
		TotalBytes:               r.totalB.Load(),
		DoneItems:                r.doneI.Load(),
		TotalItems:               r.totalI.Load(),
		ThroughputBytesPerSecond: r.lastBPS.Load(),
	}
}

func (r *Reporter) TickSnapshot() Progress {
	now := time.Now().Unix()
	done := r.doneB.Load()
	lastTick := r.lastTick.Load()
	lastBytes := r.lastBytes.Load()
	if deltaSeconds := now - lastTick; deltaSeconds > 0 && done > lastBytes {
		r.lastBPS.Store((done - lastBytes) / deltaSeconds)
		r.lastTick.Store(now)
		r.lastBytes.Store(done)
	}

	p := r.Snapshot()
	p.UpdatedAt = now
	return p
}

func (r *Reporter) Flush() error {
	return r.FlushProgress(r.Snapshot())
}

func (r *Reporter) FlushProgress(p Progress) error {
	if r.update == nil {
		return nil
	}
	return r.update(r.ctx, p)
}

func (r *Reporter) Close(final string) {
	close(r.stop)
	p := r.Snapshot()
	if r.log != nil {
		r.log.Info("%s after %s", final, FormatDuration(p.Elapsed(time.Now().Unix())))
	}
}

type CountingReadCloser struct {
	io.ReadCloser
	reporter  *Reporter
	threshold int64
	pending   int64
}

func NewCountingReadCloser(r io.ReadCloser, reporter *Reporter, threshold int64) io.ReadCloser {
	if reporter == nil {
		return r
	}
	if threshold <= 0 {
		threshold = DefaultProgressThresholdBytes
	}
	return &CountingReadCloser{ReadCloser: r, reporter: reporter, threshold: threshold}
}

func (r *CountingReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.add(int64(n))
	return n, err
}

func (r *CountingReadCloser) Close() error {
	err := r.ReadCloser.Close()
	r.flush()
	return err
}

func (r *CountingReadCloser) add(n int64) {
	if n <= 0 {
		return
	}
	r.pending += n
	if r.pending >= r.threshold {
		r.flush()
	}
}

func (r *CountingReadCloser) flush() {
	if r.pending <= 0 {
		return
	}
	r.reporter.AddBytes(r.pending)
	r.pending = 0
}
