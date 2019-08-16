package backup

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
)

func Backup(cn *pbm.PBM, node *pbm.Node) error {
	data, err := OplogTail(cn, node).Run()
	if err != nil {
		return errors.Wrap(err, "oplog tail")
	}

	for d := range data {
		fmt.Printf("%s\n", d)
	}

	return nil
}
