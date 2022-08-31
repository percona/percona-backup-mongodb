package snapshot

import (
	"io"

	"github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/mongorestore"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/percona/percona-backup-mongodb/pbm"
)

const (
	preserveUUID = true

	batchSizeDefault           = 500
	numInsertionWorkersDefault = 10
)

var ExcludeFromRestore = []string{
	pbm.DB + "." + pbm.CmdStreamCollection,
	pbm.DB + "." + pbm.LogCollection,
	pbm.DB + "." + pbm.ConfigCollection,
	pbm.DB + "." + pbm.BcpCollection,
	pbm.DB + "." + pbm.BcpOldCollection,
	pbm.DB + "." + pbm.RestoresCollection,
	pbm.DB + "." + pbm.LockCollection,
	pbm.DB + "." + pbm.LockOpCollection,
	pbm.DB + "." + pbm.PITRChunksCollection,
	pbm.DB + "." + pbm.PITRChunksOldCollection,
	pbm.DB + "." + pbm.AgentsStatusCollection,
	pbm.DB + "." + pbm.PBMOpLogCollection,
	"config.version",
	"config.mongos",
	"config.lockpings",
	"config.locks",
	"config.system.sessions",
	"config.cache.*",
	"config.shards",
	"config.transactions",
	"config.transaction_coordinators",
	"admin.system.version",
	"config.system.indexBuilds",
}

type restorer struct{ *mongorestore.MongoRestore }

func NewRestore(uri string, cfg *pbm.Config) (io.ReaderFrom, error) {
	topts := options.New("mongorestore", "0.0.1", "none", "", true, options.EnabledOptions{Auth: true, Connection: true, Namespace: true, URI: true})
	var err error
	topts.URI, err = options.NewURI(uri)
	if err != nil {
		return nil, errors.Wrap(err, "parse connection string")
	}

	err = topts.NormalizeOptionsAndURI()
	if err != nil {
		return nil, errors.Wrap(err, "parse opts")
	}

	topts.Direct = true
	topts.WriteConcern = writeconcern.New(writeconcern.WMajority())

	batchSize := batchSizeDefault
	if cfg.Restore.BatchSize > 0 {
		batchSize = cfg.Restore.BatchSize
	}
	numInsertionWorkers := numInsertionWorkersDefault
	if cfg.Restore.NumInsertionWorkers > 0 {
		numInsertionWorkers = cfg.Restore.NumInsertionWorkers
	}

	mopts := mongorestore.Options{}
	mopts.ToolOptions = topts
	mopts.InputOptions = &mongorestore.InputOptions{
		Archive: "-",
	}
	mopts.OutputOptions = &mongorestore.OutputOptions{
		BulkBufferSize:           batchSize,
		BypassDocumentValidation: true,
		Drop:                     true,
		NumInsertionWorkers:      numInsertionWorkers,
		NumParallelCollections:   1,
		PreserveUUID:             preserveUUID,
		StopOnError:              true,
		WriteConcern:             "majority",
	}
	mopts.NSOptions = &mongorestore.NSOptions{
		NSExclude: ExcludeFromRestore,
	}

	mr, err := mongorestore.New(mopts)
	if err != nil {
		return nil, errors.Wrap(err, "create mongorestore obj")
	}
	mr.SkipUsersAndRoles = true

	return &restorer{mr}, nil
}

func (r *restorer) ReadFrom(from io.Reader) (int64, error) {
	r.InputReader = from

	rdumpResult := r.Restore()
	if rdumpResult.Err != nil {
		return 0, errors.Wrapf(rdumpResult.Err, "restore mongo dump (successes: %d / fails: %d)", rdumpResult.Successes, rdumpResult.Failures)
	}

	return 0, nil
}
