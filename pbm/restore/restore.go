package restore

import (
	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
)

// Run runs the backup restore
func Run(r pbm.RestoreCmd, cn *pbm.PBM, node *pbm.Node) error {
	ver, err := node.GetMongoVersion()
	if err != nil {
		return errors.Wrap(err, "define mongo version")
	}

	_ = ver

	return nil
}
