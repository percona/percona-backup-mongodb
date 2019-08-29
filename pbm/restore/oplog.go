package restore

import (
	"io"

	"github.com/mongodb/mongo-tools-common/db"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/percona/percona-backup-mongodb/pbm"
)

func ApplyOplog(src io.ReadCloser, node *pbm.Node) error {
	// br, err := bson.NewFromIOReader(src)
	// if err != nil {
	// 	errors.Wrap(err, "create bson from reader")
	// }

	// ells, err := br.Elements()
	// if err != nil {
	// 	errors.Wrap(err, "get bson elements")
	// }

	// for _, el := range ells {
	// 	el.Value().
	// }

	bsonSource := db.NewDecodedBSONSource(db.NewBufferlessBSONSource(src))
	defer bsonSource.Close()

	for rawOplogEntry := bsonSource.LoadNext(); rawOplogEntry != nil; {
		oe := db.Oplog{}
		err := bson.Unmarshal(rawOplogEntry, &oe)
		if err != nil {
			return errors.Wrap(err, "reading oplog")
		}
	}

	return nil
}
