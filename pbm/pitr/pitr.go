package pitr

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

// IBackup is an incremental backup object
type IBackup struct {
	pbm    *pbm.PBM
	node   *pbm.Node
	rs     string
	span   time.Duration
	lastTS primitive.Timestamp
	log    *pbm.Logger
}

func NewBackup(rs string, cn *pbm.PBM, node *pbm.Node) *IBackup {
	return &IBackup{
		pbm:  cn,
		node: node,
		rs:   rs,
		span: pbm.PITRdefaultSpan,
		log:  node.Log,
	}
}

// Catchup seeks for the last saved (backuped) TS - the starting point.  It should be run only
// if the timeline was lost (e.g. on (re)start or another node's fail).
// The starting point sets to the last backup's or last PITR chunk's TS whichever is more recent
func (i *IBackup) Catchup() error {
	bcp, err := i.pbm.GetLastBackup(nil)
	if err != nil {
		return errors.Wrap(err, "get last backup")
	}
	if bcp == nil {
		return errors.New("no backup found")
	}

	rstr, err := i.pbm.GetLastRestore()
	if err != nil {
		return errors.Wrap(err, "get last restore")
	}
	if rstr != nil && rstr.StartTS > bcp.StartTS {
		return errors.Errorf("no backup found after the restored %s", rstr.Backup)
	}

	i.lastTS = bcp.LastWriteTS

	chnk, err := i.pbm.PITRLastChunkMeta(i.rs)
	if err != nil {
		return errors.Wrap(err, "get last backup")
	}

	if chnk == nil {
		return nil
	}

	if chnk.EndTS.T > i.lastTS.T {
		i.lastTS = chnk.EndTS
	}

	return nil
}

// Stream streaming (saving) chunks of the oplog to the given storage
func (i *IBackup) Stream(ctx context.Context, wakeupSig <-chan struct{}, to storage.Storage, compression pbm.CompressionType) error {
	if i.lastTS.T == 0 {
		return errors.New("no starting point defined")
	}
	i.log.Info(pbm.CmdPITR, "", "streaming started from %v / %v", time.Unix(int64(i.lastTS.T), 0).UTC(), i.lastTS.T)

	tk := time.NewTicker(i.span)
	defer tk.Stop()

	llock := &pbm.LockHeader{Replset: i.rs}
	nodeInfo, err := i.node.GetInfo()
	if err != nil {
		return errors.Wrap(err, "get NodeInfo data")
	}

	lastSlice := false

	var sliceTo primitive.Timestamp
	oplog := backup.NewOplog(i.node)
	for {
		// waiting for a trigger
		select {
		// wrapping up at the current point-in-time
		// upload the chunks up to the current time and return
		case <-ctx.Done():
			i.log.Info(pbm.CmdPITR, "", "got done signal, stopping")
			lastSlice = true
		// on wakeup or tick whatever comes first do the job
		case <-wakeupSig:
			i.log.Info(pbm.CmdPITR, "", "got wake_up signal")
		case <-tk.C:
		}

		// check if the node is still any good to make backups
		q, err := backup.NodeSuits(i.node)
		if err != nil {
			return errors.Wrap(err, "node check")
		}
		if !q {
			return nil
		}

		// before any action check if we still got a lock. if no:
		//
		// - if there another lock and it is backup op - wait for the backup to start,
		//   make the last slice up unlit backup StartTS and return
		// - if there is no other lock, we have to wait for backup - see above
		// 	 (backup cmd can delete pitr lock but might not yet acquire own one)
		// - if there another lock and that is pitr - return, probably split happened
		//   and a new worker was elected
		// - any other case (including no lock) is undefined bechavior - return
		//
		// if there is no lock, we should wait a bit for a backup lock
		// because this routine is run concurently with the snapshot
		// we don't mind to wait here and there, since in during normal (usual) flow
		// no extra waits won't occure
		//
		ld, err := i.getOpLock(llock)
		if err != nil {
			return errors.Wrap(err, "check lock")
		}

		switch ld.Type {
		case pbm.CmdPITR:
			if ld.Node != nodeInfo.Me {
				return errors.Errorf("pitr lock was stolen by node %s", ld.Node)
			}
			sliceTo, err = oplog.LastWrite()
			if err != nil {
				return errors.Wrap(err, "define last write timestamp")
			}
		case pbm.CmdBackup:
			sliceTo, err = i.backupStartTS(ld.BackupName)
			if err != nil {
				return errors.Wrap(err, "get backup start TS")
			}
			lastSlice = true
		case pbm.CmdUndefined:
			return errors.New("undefinded behaviour operation is running")
		default:
			return errors.Errorf("another operation is running: %#v", ld)
		}

		oplog.SetTailingSpan(i.lastTS, sliceTo)
		fname := i.chunkPath(i.lastTS, sliceTo, compression)
		// if use parent ctx, upload will be canceled on the "done" signal
		_, err = backup.Upload(context.Background(), oplog, to, compression, fname)
		if err != nil {
			return errors.Wrapf(err, "unable to upload chunk %v.%v", i.lastTS.T, sliceTo.T)
		}

		meta := pbm.PITRChunk{
			RS:          i.rs,
			FName:       fname,
			Compression: compression,
			StartTS:     i.lastTS,
			EndTS:       sliceTo,
		}
		err = i.pbm.PITRAddChunk(meta)
		if err != nil {
			return errors.Wrapf(err, "unable to save chunk meta %v", meta)
		}

		if lastSlice {
			i.log.Info(pbm.CmdPITR, "", "pausing/stopping with last_ts %v", time.Unix(int64(sliceTo.T), 0).UTC())
			return nil
		}

		i.lastTS = sliceTo
	}
}

func (i *IBackup) getOpLock(l *pbm.LockHeader) (ld pbm.LockData, err error) {
	tk := time.NewTicker(time.Second)
	defer tk.Stop()
	for j := 0; j < int(pbm.WaitBackupStart.Seconds()); j++ {
		ld, err = i.pbm.GetLockData(l)
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

func (i *IBackup) backupStartTS(bcp string) (ts primitive.Timestamp, err error) {
	tk := time.NewTicker(time.Second)
	defer tk.Stop()
	for j := 0; j < int(pbm.WaitBackupStart.Seconds()); j++ {
		b, err := i.pbm.GetBackupMeta(bcp)
		if err != nil {
			return ts, errors.Wrap(err, "get backup meta")
		}
		for _, rs := range b.Replsets {
			if rs.Name == i.rs && rs.FirstWriteTS.T > 1 {
				return rs.FirstWriteTS, nil
			}
		}
		<-tk.C
	}

	return ts, errors.New("run out of tries")
}

// !!! should be agreed with pbm.PITRmetaFromFName()
func (i *IBackup) chunkPath(first, last primitive.Timestamp, c pbm.CompressionType) string {
	ft := time.Unix(int64(first.T), 0).UTC()
	lt := time.Unix(int64(last.T), 0).UTC()

	name := strings.Builder{}
	if len(pbm.PITRfsPrefix) > 0 {
		name.WriteString(pbm.PITRfsPrefix)
		name.WriteString("/")
	}
	name.WriteString(i.rs)
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
