package backup

import (
	"context"
	"io"
	"time"

	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
)

func Backup(cn *pbm.PBM, node *pbm.Node) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dst, err := Destination(pbm.Storage{}, "test.pbm.oplog", pbm.CompressionTypeNo)
	if err != nil {
		return errors.Wrap(err, "storage writer")
	}

	ot := OplogTail(cn, node)
	err = ot.Run(ctx)
	go func() {
		<-time.Tick(time.Second * 5)
		cancel()
	}()
	if err != nil {
		return errors.Wrap(err, "oplog tail")
	}

	_, err = io.Copy(dst, ot)
	dst.Close()

	return errors.Wrap(err, "io copy from oplog reader to storage writer")
}
