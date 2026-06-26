package sharded

import (
	"bytes"
	"context"
	"io"
	stdlog "log"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/encrypt"
)

// pbmEncryptedMagic is the 4-byte marker every PBM-encrypted stream begins
// with. Data objects on storage must carry it when encryption is enabled.
var pbmEncryptedMagic = []byte("PBME")

// maxObjectScanBytes caps how large an object we pull off storage to inspect.
// The big ballast dump is skipped. The small per-collection dumps are enough to
// prove the storage data is encrypted.
const maxObjectScanBytes = 8 << 20  // 8 Mib

func (c *Cluster) EncryptedBackupAndRestore(confFile string, expected encrypt.EncryptionType) {
	c.ApplyConfig(context.TODO(), confFile)

	c.SetBallastData(1e5)
	checkData := c.DataChecker()

	bcpName := c.LogicalBackup()
	c.BackupWaitDone(context.TODO(), bcpName)

	c.assertBackupEncryption(context.TODO(), bcpName, expected)
	c.assertStorageEncrypted(context.TODO(), bcpName)

	c.DeleteBallast()

	stdlog.Println("resync backup list")
	if err := c.mongopbm.StoreResync(context.TODO()); err != nil {
		stdlog.Fatalln("Error: resync backup lists:", err)
	}

	c.LogicalRestore(context.TODO(), bcpName)
	checkData()
}

func (c *Cluster) EncryptedRestoreWrongPassphraseFails(correctConf, wrongConf string) {
	c.ApplyConfig(context.TODO(), correctConf)

	c.SetBallastData(1e5)
	checkData := c.DataChecker()

	bcpName := c.LogicalBackup()
	c.BackupWaitDone(context.TODO(), bcpName)

	c.DeleteBallast()

	c.ApplyConfig(context.TODO(), wrongConf)

	stdlog.Println("restoring with the wrong passphrase (expecting failure)")
	if _, err := c.pbm.Restore(bcpName, []string{}); err != nil {
		stdlog.Fatalln("starting restore with wrong passphrase:", err)
	}

	if err := c.pbm.CheckRestore(bcpName, defs.WaitBackupStart*6); err == nil {
		stdlog.Fatalln("Error: restore with wrong passphrase succeeded, expected decryption failure")
	} else {
		stdlog.Printf("restore failed as expected with wrong passphrase: %v", err)
	}

	c.clearRestoreHistory(context.TODO())

	stdlog.Println("healing cluster with the correct passphrase")
	c.ApplyConfig(context.TODO(), correctConf)
	c.LogicalRestore(context.TODO(), bcpName)
	checkData()
}

func (c *Cluster) clearRestoreHistory(ctx context.Context) {
	_, err := c.mongopbm.Conn().MongoClient().
		Database(defs.DB).
		Collection(defs.RestoresCollection).
		DeleteMany(ctx, bson.M{})
	if err != nil {
		stdlog.Fatalln("clear restore history:", err)
	}
}

func (c *Cluster) assertBackupEncryption(ctx context.Context, bcpName string, expected encrypt.EncryptionType) {
	meta, err := c.mongopbm.GetBackupMeta(ctx, bcpName)
	if err != nil {
		stdlog.Fatalln("get backup meta:", err)
	}
	if meta.Encryption != expected {
		stdlog.Fatalf("backup %s encryption mismatch: got %q, want %q", bcpName, meta.Encryption, expected)
	}
	stdlog.Printf("backup %s reports encryption %q", bcpName, meta.Encryption)
}

func (c *Cluster) assertStorageEncrypted(ctx context.Context, bcpName string) {
	stg, err := c.mongopbm.Storage(ctx)
	if err != nil {
		stdlog.Fatalln("get storage:", err)
	}

	files, err := stg.List(bcpName, "")
	if err != nil {
		stdlog.Fatalln("list backup files:", err)
	}
	if len(files) == 0 {
		stdlog.Fatalf("no files found on storage for backup %s", bcpName)
	}

	encrypted, checked, skipped := 0, 0, 0
	for _, f := range files {
		if f.Size <= 0 || f.Size > maxObjectScanBytes {
			skipped++
			continue
		}
		name := bcpName + "/" + f.Name
		r, err := stg.SourceReader(name)
		if err != nil {
			stdlog.Fatalf("read %s: %v", name, err)
		}
		// Partial reads can stall
		data, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			stdlog.Fatalf("read %s: %v", name, err)
		}
		checked++
		if bytes.HasPrefix(data, pbmEncryptedMagic) {
			encrypted++
		}
	}

	if encrypted == 0 {
		stdlog.Fatalf("no encrypted data objects found for backup %s (checked %d, skipped %d large or empty of %d)",
			bcpName, checked, skipped, len(files))
	}
	stdlog.Printf("%d/%d checked objects of backup %s carry the PBM encryption header (%d large or empty skipped)",
		encrypted, checked, bcpName, skipped)
}
