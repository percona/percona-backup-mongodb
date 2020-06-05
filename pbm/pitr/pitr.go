package pitr

import (
	"context"
	"log"
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

type IBackup struct {
	pbm    *pbm.PBM
	node   *pbm.Node
	rs     string
	span   time.Duration
	lastTS primitive.Timestamp
}

const (
	fsPrefix    = "pbmPitr"
	defaultSpan = time.Minute * 10
)

func NewBackup(rs string, pbm *pbm.PBM, node *pbm.Node) (*IBackup, error) {
	return &IBackup{
		pbm:  pbm,
		node: node,
		rs:   rs,
		span: defaultSpan,
	}, nil
}

// Catchup seeks for the last saved (backuped) TS - the starting point.  It should be run only
// if the timeline was lost (e.g. on (re)start or another node's fail).
// The starting point sets to the last backup's or last PITR chunk's TS whichever is more recent
func (i *IBackup) Catchup() error {
	bcp, err := i.pbm.GetLastBackup()
	if err != nil {
		return errors.Wrap(err, "get last backup")
	}
	if bcp == nil {
		return errors.New("no backup found")
	}

	i.lastTS = bcp.LastWriteTS

	log.Println("BCP lts", i.lastTS)

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
	log.Println("CHNK lts", i.lastTS)

	return nil
}

// Stream streaming (saving) chunks of the oplog to the given storage
func (i *IBackup) Stream(ctx context.Context, wakeupSig <-chan struct{}, to storage.Storage, compression pbm.CompressionType) error {
	if i.lastTS.T == 0 {
		return errors.New("no starting point defined")
	}

	log.Println("[INFO] PITR streaming started from", i.lastTS.T)

	tk := time.NewTicker(i.span)
	defer tk.Stop()

	llock := &pbm.LockHeader{Replset: i.rs}
	nodeInfo, err := i.node.GetIsMaster()
	if err != nil {
		return errors.Wrap(err, "get node isMaster data")
	}

	lastSlice := false

	var sliceTo primitive.Timestamp
	oplog := backup.NewOplog(i.node)
	for {
		// waiting for a trigger
		select {
		case <-ctx.Done():
			log.Println("[INFO] got done signal, stopping")
			return nil

		// on wakeup or tick whatever comes first do the job
		case <-wakeupSig:
		case <-tk.C:
		}

		// before any action check if we still got a lock. if no:
		//
		// - if there another lock and it is backup op - wait for the backup to start,
		//   make the last slice up unlit backup StartTS and return
		// - if there is no other lock, we have to wait for backup - see above
		// 	 (backup cmd can delete pitr lock but might not yet acquire own one)
		// - if there another lock and that is pitr - return, probably split happened
		//   and a new worker was elected
		// - any other case is undefined bechavior - return
		ld, err := i.pbm.GetLockData(llock, pbm.LockCollection)
		if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
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
		case pbm.CmdBackup, pbm.CmdUndefined:
			sliceTo, err = i.backupStartTS(ld.BackupName)
			if err != nil {
				return errors.Wrap(err, "get backup start TS")
			}
			lastSlice = true
		default:
			return errors.Errorf("another operation is running: %#v", ld)
		}

		oplog.SetTailingSpan(i.lastTS, sliceTo)
		fname := i.chunkPath(i.lastTS.T, sliceTo.T, compression)
		_, err = backup.Upload(ctx, oplog, to, compression, fname)
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
			return nil
		}

		i.lastTS = sliceTo
	}
}

func (i *IBackup) backupStartTS(bcp string) (ts primitive.Timestamp, err error) {
	tk := time.NewTicker(time.Second * 2)
	defer tk.Stop()
	for j := 0; j < int(pbm.WaitActionStart.Seconds()); j++ {
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

const (
	dayDiv   uint32 = 3600 * 24
	monthDiv uint32 = dayDiv * 30
)

func (i *IBackup) chunkPath(first, last uint32, c pbm.CompressionType) string {
	name := strings.Builder{}
	name.WriteString(fsPrefix)
	name.WriteString("/")
	name.WriteString(i.rs)
	name.WriteString("/")
	name.WriteString(strconv.Itoa(int(first / monthDiv)))
	name.WriteString("/")
	name.WriteString(strconv.Itoa(int(first / dayDiv)))
	name.WriteString("/")
	name.WriteString(strconv.Itoa(int(first)))
	name.WriteString(".")
	name.WriteString(strconv.Itoa(int(last)))
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
