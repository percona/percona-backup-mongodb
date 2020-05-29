package pitr

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"

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
func (i *IBackup) Stream(ctx context.Context, to storage.Storage, compression pbm.CompressionType) error {
	if i.lastTS.T == 0 {
		return errors.New("no starting point defined")
	}

	log.Println("[INFO] PITR streaming started from", i.lastTS.T)

	tk := time.NewTicker(i.span)
	defer tk.Stop()

	oplog := backup.NewOplog(i.node)
	for {
		select {
		case <-tk.C:
			now, err := oplog.LastWrite()
			if err != nil {
				return errors.Wrap(err, "define last write timestamp")
			}

			oplog.SetTailingSpan(i.lastTS, now)
			fname := i.chunkPath(i.lastTS.T, now.T, compression)
			_, err = backup.Upload(ctx, oplog, to, compression, fname)
			if err != nil {
				return errors.Wrapf(err, "unable to upload chunk %v.%v", i.lastTS.T, now.T)
			}

			meta := pbm.PITRChunk{
				RS:          i.rs,
				FName:       fname,
				Compression: compression,
				StartTS:     i.lastTS,
				EndTS:       now,
			}
			err = i.pbm.PITRAddChunk(meta)
			if err != nil {
				return errors.Wrapf(err, "unable to save chunk meta %v", meta)
			}

			i.lastTS = now
		case <-ctx.Done():
			return nil
		}
	}
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
