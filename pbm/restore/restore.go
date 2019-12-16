package restore

import (
	"encoding/json"

	"github.com/mongodb/mongo-tools-common/db"
	"github.com/mongodb/mongo-tools-common/options"
	"github.com/mongodb/mongo-tools/mongorestore"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
)

var excludeFromDumpRestore = []string{
	pbm.DB + "." + pbm.CmdStreamCollection,
	pbm.DB + "." + pbm.LogCollection,
	pbm.DB + "." + pbm.ConfigCollection,
	pbm.DB + "." + pbm.BcpCollection,
	pbm.DB + "." + pbm.LockCollection,
	"config.version",
	"config.mongos",
	"admin.system.version",
}

// Run runs the backup restore
func Run(r pbm.RestoreCmd, cn *pbm.PBM, node *pbm.Node) error {
	stg, err := cn.GetStorage()
	if err != nil {
		return errors.Wrap(err, "get backup store")
	}

	bcp, err := cn.GetBackupMeta(r.BackupName)
	if errors.Cause(err) == mongo.ErrNoDocuments {
		bcp, err = getMetaFromStore(r.BackupName, stg)
	}
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

	var (
		rsMeta pbm.BackupReplset
		ok     bool
	)
	for _, v := range bcp.Replsets {
		if v.Name == rsName {
			rsMeta = v
			ok = true
		}
	}
	if !ok {
		return errors.Errorf("metadata for replset/shard %s is not found", rsName)
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
			Drop:                     true,
			NumInsertionWorkers:      20,
			NumParallelCollections:   1,
			PreserveUUID:             preserveUUID,
			StopOnError:              true,
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
	mr.Close()

	oplogReader, oplogCloser, err := Source(stg, rsMeta.OplogName, bcp.Compression) //pbm.CompressionTypeNone)
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

func getMetaFromStore(bcpName string, stg pbm.Storage) (*pbm.BackupMeta, error) {
	rr, _, err := Source(stg, bcpName+".pbm.json", pbm.CompressionTypeNone)
	if err != nil {
		return nil, errors.Wrap(err, "get from store")
	}

	b := &pbm.BackupMeta{}
	err = json.NewDecoder(rr).Decode(b)

	return b, errors.Wrap(err, "decode")
}
