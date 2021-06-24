package pitr

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

// Slicer is an incremental backup object
type Slicer struct {
	pbm     *pbm.PBM
	node    *pbm.Node
	rs      string
	span    int64
	lastTS  primitive.Timestamp
	storage storage.Storage
	oplog   *backup.Oplog
	l       *log.Event
	ep      pbm.Epoch
}

// NewSlicer creates an incremental backup object
func NewSlicer(rs string, cn *pbm.PBM, node *pbm.Node, to storage.Storage, ep pbm.Epoch) *Slicer {
	return &Slicer{
		pbm:     cn,
		node:    node,
		rs:      rs,
		span:    int64(pbm.PITRdefaultSpan),
		storage: to,
		oplog:   backup.NewOplog(node),
		l:       cn.Logger().NewEvent(string(pbm.CmdPITR), "", "", ep.TS()),
		ep:      ep,
	}
}

// SetSpan sets span duration. Streaming will recognise the change and adjust on the next iteration.
func (s *Slicer) SetSpan(d time.Duration) {
	atomic.StoreInt64(&s.span, int64(d))
}

// SetSpan sets span duration. Streaming will recognise the change and adjust on the next iteration.
func (s *Slicer) GetSpan() time.Duration {
	return time.Duration(atomic.LoadInt64(&s.span))
}

// Catchup seeks for the last saved (backuped) TS - the starting point. It should be run only
// if the timeline was lost (e.g. on (re)start, restart after backup, node's fail).
// The starting point sets to the last backup's or last PITR chunk's TS whichever is the most recent.
// If there is a chunk behind the last backup it will try to fill the gaps from the chunk to the starting point.
// While filling gaps it checks the oplog for sufficiency. It also checks if there is no restore intercepted
// the timeline (hence there are no restores after the most recent backup)
func (s *Slicer) Catchup() error {
	baseBcp, err := s.pbm.GetLastBackup(nil)
	if err != nil {
		return errors.Wrap(err, "get last backup")
	}
	if baseBcp == nil {
		return errors.New("no backup found, a new backup is required to start PITR")
	}

	defer func() {
		s.l.Debug("lastTS set to %v %s", s.lastTS, formatts(s.lastTS))
	}()

	rstr, err := s.pbm.GetLastRestore()
	if err != nil {
		return errors.Wrap(err, "get last restore")
	}
	if rstr != nil && rstr.StartTS > baseBcp.StartTS {
		return errors.Errorf("no backup found after the restored %s, a new backup is required to resume PITR", rstr.Backup)
	}

	chnk, err := s.pbm.PITRLastChunkMeta(s.rs)
	if err != nil {
		return errors.Wrap(err, "get last slice")
	}

	s.lastTS = baseBcp.LastWriteTS

	if chnk == nil {
		return nil
	}

	// PITR chunk after the recent backup is the most recent oplog slice
	if primitive.CompareTimestamp(chnk.EndTS, baseBcp.LastWriteTS) >= 0 {
		s.lastTS = chnk.EndTS
		return nil
	}

	if rstr != nil && rstr.StartTS > int64(chnk.StartTS.T) {
		s.l.Info("restore after %s the chunk %s, skip", rstr.Backup, chnk)
		return nil
	}

	bl, err := s.pbm.BackupsDoneList(&chnk.EndTS, 0, -1)
	if err != nil {
		return errors.Wrapf(err, "get backups list from %v", chnk.EndTS)
	}

	if len(bl) > 1 {
		s.l.Debug("chunk too far (more than a one snapshot)")
		return nil
	}

	// if there is a gap between chunk and the backup - fill it
	// failed gap shouldn't prevent further chunk creation
	if primitive.CompareTimestamp(chnk.EndTS, baseBcp.FirstWriteTS) < 0 {
		ok, err := s.oplog.IsSufficient(chnk.EndTS)
		if err != nil {
			s.l.Warning("check oplog sufficiency for %s: %v", chnk, err)
			return nil
		}
		if !ok {
			s.l.Info("insufficient range since %v", chnk.EndTS)
			return nil
		}

		err = s.upload(chnk.EndTS, baseBcp.FirstWriteTS, chnk.Compression)
		if err != nil {
			s.l.Warning("create last_chunk<->sanpshot slice: %v", err)
			return nil
		}
		s.l.Info("created chunk %s - %s", formatts(chnk.EndTS), formatts(baseBcp.FirstWriteTS))
	}

	err = s.copyFromBcp(baseBcp)
	if err != nil {
		s.l.Warning("copy snapshot [%s] oplog: %v", baseBcp.Name, err)
	} else {
		s.l.Info("copied chunk %s - %s", formatts(baseBcp.FirstWriteTS), formatts(baseBcp.LastWriteTS))
	}

	return nil
}

func (s *Slicer) copyFromBcp(bcp *pbm.BackupMeta) error {
	var oplog string
	for _, r := range bcp.Replsets {
		if r.Name == s.rs {
			oplog = r.OplogName
			break
		}
	}
	if oplog == "" {
		return errors.New("no data for shard")
	}

	n := s.chunkPath(bcp.FirstWriteTS, bcp.LastWriteTS, bcp.Compression)
	err := s.storage.Copy(oplog, n)
	if err != nil {
		return errors.Wrap(err, "storage copy")
	}

	meta := pbm.PITRChunk{
		RS:          s.rs,
		FName:       n,
		Compression: bcp.Compression,
		StartTS:     bcp.FirstWriteTS,
		EndTS:       bcp.LastWriteTS,
	}
	err = s.pbm.PITRAddChunk(meta)
	if err != nil {
		return errors.Wrapf(err, "unable to save chunk meta %v", meta)
	}

	return nil
}

// ErrOpMoved is the error signaling that slicing op
// now being run by the other node
type ErrOpMoved struct {
	to string
}

func (e ErrOpMoved) Error() string {
	return fmt.Sprintf("pitr slicing resumed on node %s", e.to)
}

// LogStartMsg message to log on successful streaming start
const LogStartMsg = "start_ok"

// Stream streaming (saving) chunks of the oplog to the given storage
func (s *Slicer) Stream(ctx context.Context, wakeupSig <-chan struct{}, compression pbm.CompressionType) error {
	if s.lastTS.T == 0 {
		return errors.New("no starting point defined")
	}
	s.l.Debug(LogStartMsg)
	s.l.Info("streaming started from %v / %v", time.Unix(int64(s.lastTS.T), 0).UTC(), s.lastTS.T)

	cspan := s.GetSpan()
	tk := time.NewTicker(cspan)
	defer tk.Stop()

	nodeInfo, err := s.node.GetInfo()
	if err != nil {
		return errors.Wrap(err, "get NodeInfo data")
	}

	lastSlice := false
	llock := &pbm.LockHeader{Replset: s.rs}

	var sliceTo primitive.Timestamp
	for {
		// waiting for a trigger
		select {
		// wrapping up at the current point-in-time
		// upload the chunks up to the current time and return
		case <-ctx.Done():
			s.l.Info("got done signal, stopping")
			lastSlice = true
		// on wakeup or tick whatever comes first do the job
		case <-wakeupSig:
			s.l.Info("got wake_up signal")
		case <-tk.C:
		}

		nextChunkT := time.Now().Add(cspan)

		// if this is the last slice, epoch probably already changed (e.g. due to config changes) and that's ok
		if !lastSlice {
			cep, err := s.pbm.GetEpoch()
			if err != nil {
				return errors.Wrap(err, "get epoch")
			}
			if primitive.CompareTimestamp(s.ep.TS(), cep.TS()) != 0 {
				return errors.Errorf("epoch mismatch. Got sleep in %v, woke up in %v. Too old for that stuff.", s.ep.TS(), cep.TS())
			}
		}

		// check if the node is still any good to make backups
		ninf, err := s.node.GetInfo()
		if err != nil {
			return errors.Wrap(err, "get node info")
		}
		q, err := backup.NodeSuits(s.node, ninf)
		if err != nil {
			return errors.Wrap(err, "node check")
		}
		if !q {
			return nil
		}

		// before any action check if we still got a lock. if no:
		//
		// - if there is another lock and it is the backup operation - wait for the backup
		//   to start, make the last slice up unlit backup StartTS and return;
		// - if there is no other lock, we have to wait for the snapshot backup - see above
		//   (snapshot cmd can delete pitr lock but might not yet acquire the own one)
		// - if there another lock and that is pitr - return, probably the split happened
		//   and a new worker was elected
		// - any other case (including no lock) is the undefined behaviour - return
		//
		// if there is no lock, we should wait a bit for a backup lock
		// because this routine is run concurently with the snapshot
		// we don't mind to wait here and there, since in during normal (usual) flow
		// no extra waits won't occure
		//
		ld, err := s.getOpLock(llock)
		if err != nil {
			return errors.Wrap(err, "check lock")
		}

		// in case there is a lock, even legit (our own, or backup's one) but it is stale
		// we sould return so the slicer whould get thru the lock aquisition again.
		ts, err := s.pbm.ClusterTime()
		if err != nil {
			return errors.Wrap(err, "read cluster time")
		}
		if ld.Heartbeat.T+pbm.StaleFrameSec < ts.T {
			return errors.Errorf("stale lock %#v, last beat ts: %d", ld.LockHeader, ld.Heartbeat.T)
		}

		switch ld.Type {
		case pbm.CmdPITR:
			if ld.Node != nodeInfo.Me {
				return ErrOpMoved{ld.Node}
			}
			sliceTo, err = s.oplog.LastWrite()
			if err != nil {
				return errors.Wrap(err, "define last write timestamp")
			}
		case pbm.CmdBackup:
			sliceTo, err = s.backupStartTS(ld.OPID)
			if err != nil {
				return errors.Wrap(err, "get backup start TS")
			}
			lastSlice = true
		case pbm.CmdUndefined:
			return errors.New("undefinded behaviour operation is running")
		default:
			return errors.Errorf("another operation is running: %#v", ld)
		}

		err = s.upload(s.lastTS, sliceTo, compression)
		if err != nil {
			return err
		}

		logm := fmt.Sprintf("created chunk %s - %s", formatts(s.lastTS), formatts(sliceTo))
		if !lastSlice {
			logm += fmt.Sprintf(". Next chunk creation scheduled to begin at ~%s", nextChunkT.Format("2006-01-02T15:04:05"))
		}
		s.l.Info(logm)

		if lastSlice {
			s.l.Info("pausing/stopping with last_ts %v", time.Unix(int64(sliceTo.T), 0).UTC())
			return nil
		}

		s.lastTS = sliceTo

		if ispan := s.GetSpan(); cspan != ispan {
			tk.Reset(ispan)
			cspan = ispan
		}
	}
}

func (s *Slicer) upload(from, to primitive.Timestamp, compression pbm.CompressionType) error {
	s.oplog.SetTailingSpan(from, to)
	fname := s.chunkPath(from, to, compression)
	// if use parent ctx, upload will be canceled on the "done" signal
	_, err := backup.Upload(context.Background(), s.oplog, s.storage, compression, fname, -1)
	if err != nil {
		// PITR chunks have no metadata to indicate any failed state and if something went
		// wrong during the data read we may end up with an already created file. Although
		// the failed range won't be saved in db as the available for restore. It would get
		// in there after the storage resync. see: https://jira.percona.com/browse/PBM-602
		s.l.Debug("remove %s due to upload errors", fname)
		derr := s.storage.Delete(fname)
		if derr != nil {
			s.l.Error("remove %s: %v", fname, derr)
		}
		return errors.Wrapf(err, "unable to upload chunk %v.%v", from, to)
	}

	meta := pbm.PITRChunk{
		RS:          s.rs,
		FName:       fname,
		Compression: compression,
		StartTS:     from,
		EndTS:       to,
	}
	err = s.pbm.PITRAddChunk(meta)
	if err != nil {
		return errors.Wrapf(err, "unable to save chunk meta %v", meta)
	}

	return nil
}

func formatts(t primitive.Timestamp) string {
	return time.Unix(int64(t.T), 0).UTC().Format("2006-01-02T15:04:05")
}

func (s *Slicer) getOpLock(l *pbm.LockHeader) (ld pbm.LockData, err error) {
	tk := time.NewTicker(time.Second)
	defer tk.Stop()
	for j := 0; j < int(pbm.WaitBackupStart.Seconds()); j++ {
		ld, err = s.pbm.GetLockData(l)
		if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
			return ld, errors.Wrap(err, "get")
		}
		if ld.Type != pbm.CmdUndefined {
			return ld, nil
		}
		<-tk.C
	}

	return ld, nil
}

func (s *Slicer) backupStartTS(opid string) (ts primitive.Timestamp, err error) {
	tk := time.NewTicker(time.Second)
	defer tk.Stop()
	for j := 0; j < int(pbm.WaitBackupStart.Seconds()); j++ {
		b, err := s.pbm.GetBackupByOPID(opid)
		if err != nil {
			return ts, errors.Wrap(err, "get backup meta")
		}
		for _, rs := range b.Replsets {
			if rs.Name == s.rs && rs.FirstWriteTS.T > 1 {
				return rs.FirstWriteTS, nil
			}
		}
		<-tk.C
	}

	return ts, errors.New("run out of tries")
}

// !!! should be agreed with pbm.PITRmetaFromFName()
func (s *Slicer) chunkPath(first, last primitive.Timestamp, c pbm.CompressionType) string {
	ft := time.Unix(int64(first.T), 0).UTC()
	lt := time.Unix(int64(last.T), 0).UTC()

	name := strings.Builder{}
	if len(pbm.PITRfsPrefix) > 0 {
		name.WriteString(pbm.PITRfsPrefix)
		name.WriteString("/")
	}
	name.WriteString(s.rs)
	name.WriteString("/")
	name.WriteString(ft.Format("20060102"))
	name.WriteString("/")
	name.WriteString(ft.Format("20060102150405"))
	name.WriteString("-")
	name.WriteString(strconv.Itoa(int(first.I)))
	name.WriteString(".")
	name.WriteString(lt.Format("20060102150405"))
	name.WriteString("-")
	name.WriteString(strconv.Itoa(int(last.I)))
	name.WriteString(".oplog")
	name.WriteString(csuffix(c))

	return name.String()
}

func csuffix(c pbm.CompressionType) string {
	switch c {
	case pbm.CompressionTypeGZIP, pbm.CompressionTypePGZIP:
		return ".gz"
	case pbm.CompressionTypeLZ4:
		return ".lz4"
	case pbm.CompressionTypeSNAPPY, pbm.CompressionTypeS2:
		return ".snappy"
	default:
		return ""
	}
}
