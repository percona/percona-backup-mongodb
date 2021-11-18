package backup

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"

	"github.com/percona/percona-backup-mongodb/pbm"
	plog "github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

type Meta struct {
	ID           UUID                `bson:"backupId"`
	DBpath       string              `bson:"dbpath"`
	OplogStart   BCoplogTS           `bson:"oplogStart"`
	OplogEnd     BCoplogTS           `bson:"oplogEnd"`
	CheckpointTS primitive.Timestamp `bson:"checkpointTimestamp"`
}

type BCoplogTS struct {
	TS primitive.Timestamp `bson:"ts"`
	T  int64               `bson:"t"`
}

type BackupCursorData struct {
	Meta *Meta
	Data []pbm.BCfile
}
type BackupCursor struct {
	id    UUID
	n     *pbm.Node
	l     *plog.Event
	close chan struct{}
}

func NewBackupCursor(n *pbm.Node, l *plog.Event) *BackupCursor {
	return &BackupCursor{
		n: n,
		l: l,
	}
}

func (bc *BackupCursor) Data(ctx context.Context) (bcp *BackupCursorData, err error) {
	cur, err := bc.n.Session().Database("admin").Aggregate(ctx, mongo.Pipeline{
		{{"$backupCursor", bson.D{}}},
	})
	if err != nil {
		return nil, errors.Wrap(err, "create backupCursor")
	}
	defer func() {
		if err != nil {
			cur.Close(ctx)
		}
	}()

	var m *Meta
	var files []pbm.BCfile
	for cur.TryNext(ctx) {
		// metadata is the first
		if m == nil {
			mc := struct {
				Data Meta `bson:"metadata"`
			}{}
			err = cur.Decode(&mc)
			if err != nil {
				return nil, errors.Wrap(err, "decode metadata")
			}
			m = &mc.Data
			continue
		}

		var d pbm.BCfile
		err = cur.Decode(&d)
		if err != nil {
			return nil, errors.Wrap(err, "decode filename")
		}

		files = append(files, d)
	}

	bc.id = m.ID

	bc.close = make(chan struct{})
	go func() {
		tk := time.NewTicker(time.Minute * 1)
		defer tk.Stop()
		for {
			select {
			case <-bc.close:
				bc.l.Debug("stop cursor polling: %v, cursor err: %v",
					cur.Close(ctx), cur.Err())
				return
			case <-tk.C:
				cur.TryNext(ctx)
				bc.l.Debug("do_cursor_poll")
			}
		}
	}()

	return &BackupCursorData{m, files}, nil
}

func (bc *BackupCursor) Journals(upto primitive.Timestamp) ([]pbm.BCfile, error) {
	ctx := context.Background()
	cur, err := bc.n.Session().Database("admin").Aggregate(ctx,
		mongo.Pipeline{
			{{"$backupCursorExtend", bson.D{{"backupId", bc.id}, {"timestamp", upto}}}},
		})
	if err != nil {
		return nil, errors.Wrap(err, "create backupCursorExtend")
	}
	defer cur.Close(ctx)

	var j []pbm.BCfile

	err = cur.All(ctx, &j)
	return j, err
}

func (bc *BackupCursor) Close() {
	if bc.close != nil {
		bc.close <- struct{}{}
	}
}

// run physical backup.
// TODO: describe flow
func (b *Backup) runPhysical(ctx context.Context, bcp pbm.BackupCmd, opid pbm.OPID, l *plog.Event) (err error) {
	inf, err := b.node.GetInfo()
	if err != nil {
		return errors.Wrap(err, "get cluster info")
	}

	rsName := inf.SetName

	rsMeta := pbm.BackupReplset{
		Name:         rsName,
		StartTS:      time.Now().UTC().Unix(),
		Status:       pbm.StatusRunning,
		Conditions:   []pbm.Condition{},
		FirstWriteTS: primitive.Timestamp{T: 1, I: 1},
	}

	stg, err := b.cn.GetStorage(l)
	if err != nil {
		return errors.Wrap(err, "unable to get PBM storage configuration settings")
	}

	bcpm, err := b.cn.GetBackupMeta(bcp.Name)
	if err != nil {
		return errors.Wrap(err, "get backup meta")
	}

	// on any error the RS' and the backup' (in case this is the backup leader) meta will be marked aproprietly
	defer func() {
		if err != nil {
			status := pbm.StatusError
			if errors.Is(err, ErrCancelled) {
				status = pbm.StatusCancelled

				meta := &pbm.BackupMeta{
					Name:     bcp.Name,
					Status:   pbm.StatusCancelled,
					Replsets: []pbm.BackupReplset{rsMeta},
				}

				l.Info("delete artefacts from storage: %v", b.cn.DeleteBackupFiles(meta, stg))
			}

			ferr := b.cn.ChangeRSState(bcp.Name, rsMeta.Name, status, err.Error())
			l.Info("mark RS as %s `%v`: %v", status, err, ferr)

			if inf.IsLeader() {
				ferr := b.cn.ChangeBackupState(bcp.Name, status, err.Error())
				l.Info("mark backup as %s `%v`: %v", status, err, ferr)
			}
		}

		// Turn the balancer back on if needed
		//
		// Every agent will check if the balancer was on before the backup started.
		// And will try to turn it on again if so. So if the leader node went down after turning off
		// the balancer some other node will bring it back.
		// TODO: what if all agents went down.
		if bcpm.BalancerStatus != pbm.BalancerModeOn {
			return
		}

		errd := b.cn.SetBalancerStatus(pbm.BalancerModeOn)
		if errd != nil {
			l.Error("set balancer ON: %v", errd)
			return
		}
		l.Debug("set balancer on")
	}()

	if inf.IsLeader() {
		hbstop := make(chan struct{})
		defer close(hbstop)
		err := b.cn.BackupHB(bcp.Name)
		if err != nil {
			return errors.Wrap(err, "init heartbeat")
		}
		go func() {
			tk := time.NewTicker(time.Second * 5)
			defer tk.Stop()
			for {
				select {
				case <-tk.C:
					err := b.cn.BackupHB(bcp.Name)
					if err != nil {
						l.Error("send pbm heartbeat: %v", err)
					}
				case <-hbstop:
					return
				}
			}
		}()

		if bcpm.BalancerStatus == pbm.BalancerModeOn {
			err = b.cn.SetBalancerStatus(pbm.BalancerModeOff)
			if err != nil {
				return errors.Wrap(err, "set balancer OFF")
			}
			l.Debug("waiting for balancer off")
			bs := waitForBalancerOff(b.cn, time.Second*30, l)
			l.Debug("balancer status: %s", bs)
		}
	}

	// Waiting for StatusStarting to move further.
	// In case some preparations has to be done before backup.
	err = b.waitForStatus(bcp.Name, pbm.StatusStarting, &pbm.WaitBackupStart)
	if err != nil {
		return errors.Wrap(err, "waiting for start")
	}

	cursor := NewBackupCursor(b.node, l)
	bcur, err := cursor.Data(ctx)
	if err != nil {
		return errors.Wrap(err, "get backup files")
	}
	defer cursor.Close()

	l.Debug("backup cursor id: %s", bcur.Meta.ID)

	lwts, err := pbm.LastWrite(b.node.Session(), true)
	if err != nil {
		return errors.Wrap(err, "get shard's last write ts")
	}

	rsMeta.Status = pbm.StatusRunning
	rsMeta.FirstWriteTS = bcur.Meta.OplogEnd.TS
	rsMeta.LastWriteTS = lwts
	// rsMeta.PhyData = bcur.Data // TODO: need to add journals
	err = b.cn.AddRSMeta(bcp.Name, rsMeta)
	if err != nil {
		return errors.Wrap(err, "add shard's metadata")
	}

	if inf.IsLeader() {
		err := b.reconcileStatus(bcp.Name, opid.String(), pbm.StatusRunning, inf, &pbm.WaitBackupStart)
		if err != nil {
			if errors.Cause(err) == errConvergeTimeOut {
				return errors.Wrap(err, "couldn't get response from all shards")
			}
			return errors.Wrap(err, "check cluster for backup started")
		}

		err = b.setClusterFirstWrite(bcp.Name)
		if err != nil {
			return errors.Wrap(err, "set cluster first write ts")
		}

		err = b.setClusterLastWrite(bcp.Name)
		if err != nil {
			return errors.Wrap(err, "set cluster last write ts")
		}
	}

	// Waiting for cluster's StatusRunning to move further.
	err = b.waitForStatus(bcp.Name, pbm.StatusRunning, nil)
	if err != nil {
		return errors.Wrap(err, "waiting for running")
	}

	_, lwTS, err := b.waitForFirstLastWrite(bcp.Name)
	if err != nil {
		return errors.Wrap(err, "get cluster first & last write ts")
	}

	l.Debug("set journal up to %v", lwTS)

	jrnls, err := cursor.Journals(lwTS)
	if err != nil {
		return errors.Wrap(err, "get journal files")
	}

	subd := bcp.Name + "/" + rsName
	for _, bd := range bcur.Data {
		l.Debug("uploading data: %s", bd.File)
		err = writeFile(bd.File, subd+"/"+strings.TrimPrefix(bd.File, bcur.Meta.DBpath+"/"), stg)
		if err != nil {
			return errors.Wrapf(err, "upload data file %s", bd.File)
		}
	}
	l.Debug("finished uploading data")

	for _, jf := range jrnls {
		l.Debug("uploading journal: %s", jf.File)
		err = writeFile(jf.File, subd+"/"+strings.TrimPrefix(jf.File, bcur.Meta.DBpath+"/"), stg)
		if err != nil {
			return errors.Wrapf(err, "upload journal file %s", jf.File)
		}
	}
	l.Debug("finished uploading journals")

	err = b.cn.ChangeRSState(bcp.Name, rsMeta.Name, pbm.StatusDone, "")
	if err != nil {
		return errors.Wrap(err, "set shard's StatusDone")
	}

	if inf.IsLeader() {
		epch, err := b.cn.ResetEpoch()
		if err != nil {
			l.Error("reset epoch")
		} else {
			l.Debug("epoch set to %v", epch)
		}

		err = b.reconcileStatus(bcp.Name, opid.String(), pbm.StatusDone, inf, nil)
		if err != nil {
			return errors.Wrap(err, "check cluster for backup done")
		}

		err = b.dumpClusterMeta(bcp.Name, stg)
		if err != nil {
			return errors.Wrap(err, "dump metadata")
		}
	}

	l.Debug("waiting status: %s", pbm.StatusDone)
	// to be sure the locks released only after the "done" status had written
	err = b.waitForStatus(bcp.Name, pbm.StatusDone, nil)
	return errors.Wrap(err, "waiting for done")
}

// UUID represents a UUID as saved in MongoDB
type UUID struct{ uuid.UUID }

// MarshalBSONValue implements the bson.ValueMarshaler interface.
func (id UUID) MarshalBSONValue() (bsontype.Type, []byte, error) {
	return bsontype.Binary, bsoncore.AppendBinary(nil, 4, id.UUID[:]), nil
}

// UnmarshalBSONValue implements the bson.ValueUnmarshaler interface.
func (id *UUID) UnmarshalBSONValue(t bsontype.Type, raw []byte) error {
	if t != bsontype.Binary {
		return fmt.Errorf("invalid format on unmarshal bson value")
	}

	_, data, _, ok := bsoncore.ReadBinary(raw)
	if !ok {
		return fmt.Errorf("not enough bytes to unmarshal bson value")
	}

	copy(id.UUID[:], data)

	return nil
}

// IsZero implements the bson.Zeroer interface.
func (id *UUID) IsZero() bool {
	return bytes.Equal(id.UUID[:], uuid.Nil[:])
}

func writeFile(src, dst string, stg storage.Storage) error {
	f, err := os.Open(src)
	if err != nil {
		return errors.Wrap(err, "open file for reading")
	}
	defer f.Close()
	fstat, err := os.Stat(src)
	if err != nil {
		return errors.Wrap(err, "get file stat")
	}
	err = stg.Save(dst, f, int(fstat.Size()))
	if err != nil {
		return errors.Wrap(err, "upload file")
	}

	return nil
}
