package restore

import (
	"github.com/mongodb/mongo-tools-common/db"
	"github.com/mongodb/mongo-tools-common/options"
	"github.com/mongodb/mongo-tools/mongorestore"
	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
)

var excludeFromDumpRestore = []string{
	pbm.CmdStreamDB + "." + pbm.CmdStreamCollection,
	pbm.DB + "." + pbm.LogCollection,
	pbm.DB + "." + pbm.ConfigCollection,
	pbm.DB + "." + pbm.BcpCollection,
	pbm.DB + "." + pbm.OpCollection,
}

// Run runs the backup restore
func Run(r pbm.RestoreCmd, cn *pbm.PBM, node *pbm.Node) error {
	bcp, err := cn.GetBackupMeta(r.BackupName)
	if err != nil {
		return errors.Wrap(err, "get backup metadata")
	}

	if bcp.Status != pbm.StatusDone {
		return errors.Errorf("backup wasn't successfull: starus: %s, error: %s", bcp.Status, bcp.Error)
	}

	im, err := node.GetIsMaster()
	if err != nil {
		return errors.Wrap(err, "get isMaster data")
	}
	rsName := im.SetName
	if rsName == "" {
		rsName = pbm.NoReplset
	}
	rsMeta, ok := bcp.Replsets[rsName]
	if !ok {
		return errors.Errorf("metadata form replset/shard %s is not found", rsName)
	}

	stg, err := cn.GetStorage()
	if err != nil {
		return errors.Wrap(err, "get backup store")
	}

	ver, err := node.GetMongoVersion()
	if err != nil || len(ver.Version) < 1 {
		return errors.Wrap(err, "define mongo version")
	}

	dumpReader, dumpCloser, err := Source(stg, rsMeta.DumpName, pbm.CompressionTypeNone) //, bcp.Compression)
	if err != nil {
		return errors.Wrap(err, "create source object for the dump restore")
	}
	defer func() {
		dumpReader.Close()
		if dumpCloser != nil {
			dumpCloser.Close()
		}
	}()

	preserveUUID := true
	dropCollections := true
	ignoreErrors := false

	if im.ConfigSvr == 2 && im.SetName != "" {
		// For config servers. If dropCollections is true, we are going to receive error
		// "cannot drop config.version document while in --configsvr mode"
		// We must ignore the errors and we shouldn't drop the collections
		dropCollections = false
		ignoreErrors = true
		preserveUUID = false // cannot be used with dropCollections=false
	}
	if ver.Version[0] < 4 {
		preserveUUID = false
	}

	topts := options.ToolOptions{
		AppName:    "mongodump",
		VersionStr: "0.0.1",
		URI:        &options.URI{ConnectionString: node.ConnURI()},
		Auth:       &options.Auth{},
		Namespace:  &options.Namespace{},
		Connection: &options.Connection{},
		Direct:     true,
	}

	rsession, err := db.NewSessionProvider(topts)
	if err != nil {
		return errors.Wrap(err, "create session for the dump restore")
	}

	mr := mongorestore.MongoRestore{
		SessionProvider: rsession,
		ToolOptions:     &topts,
		InputOptions: &mongorestore.InputOptions{
			Gzip:    bcp.Compression == pbm.CompressionTypeGZIP,
			Archive: "-",
		},
		OutputOptions: &mongorestore.OutputOptions{
			BulkBufferSize:           2000,
			BypassDocumentValidation: true,
			Drop:                     dropCollections,
			NumInsertionWorkers:      20,
			NumParallelCollections:   4,
			PreserveUUID:             preserveUUID,
			StopOnError:              !ignoreErrors,
			TempRolesColl:            "temproles",
			TempUsersColl:            "tempusers",
			WriteConcern:             "majority",
		},
		NSOptions: &mongorestore.NSOptions{
			NSExclude: excludeFromDumpRestore,
		},
		InputReader: dumpReader,
	}

	rdumpResult := mr.Restore()
	if rdumpResult.Err != nil {
		return errors.Wrapf(rdumpResult.Err, "restore mongo dump (successes: %d / fails: %d)", rdumpResult.Successes, rdumpResult.Failures)
	}

	oplogReader, oplogCloser, err := Source(stg, rsMeta.OplogName, pbm.CompressionTypeNone)
	if err != nil {
		return errors.Wrap(err, "create source object for the oplog restore")
	}
	defer func() {
		oplogReader.Close()
		if oplogCloser != nil {
			oplogCloser.Close()
		}
	}()

	err = NewOplog(node, ver, preserveUUID).Apply(oplogReader)

	return errors.Wrap(err, "apply oplog")
}
