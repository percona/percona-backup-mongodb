package backup

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/internal/compress"
	"github.com/percona/percona-backup-mongodb/internal/errors"
)

func FormatChunkName(from, till primitive.Timestamp, cmp compress.CompressionType) string {
	return fmt.Sprintf("%d.%d-%d.%d%s", from.T, from.I, till.T, till.I, cmp.Suffix())
}

//nolint:nonamedreturns
func ParseChunkName(name string) (from, till primitive.Timestamp, cmp compress.CompressionType, err error) {
	var c string
	fmt.Sscanf(name, "%d.%d-%d.%d.%s", &from.T, &from.I, &till.T, &till.I, &c)

	if from.T == 0 || from.I == 0 || till.T == 0 || till.I == 0 {
		err = errors.Errorf("failed to parse %q", name)
		return
	}

	if c != "" {
		cmp = compress.FileCompression(c)
		if cmp == "" {
			err = errors.Errorf("failed to parse %q", name)
			return
		}
	}

	return
}
