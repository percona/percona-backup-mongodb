package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/percona/percona-backup-mongodb/pbm/util"
	"github.com/percona/percona-backup-mongodb/wt"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatal("invalid number of argument")
	}
	src, dst := os.Args[1], os.Args[2]

	info, err := getBackupInfo(src)
	if err != nil {
		log.Fatalf("get backup info: %v", err)
	}

	nss := []*wt.Namespace{}
	isSelected := util.MakeSelectedPred([]string{"db0", "db1.c1"})
	for _, m := range info {
		if isSelected(m.NS) {
			nss = append(nss, m)
		}
	}

	if err := doImport(src, dst, nss); err != nil {
		log.Fatalf("import: %v", err)
	}
}

func getBackupInfo(path string) ([]*wt.Namespace, error) {
	temp, err := os.MkdirTemp("", "")
	if err != nil {
		return nil, fmt.Errorf("mkdtemp: %w", err)
	}
	defer os.RemoveAll(temp)

	for _, filename := range wt.BackupFiles {
		a := filepath.Join(path, filename)
		b := filepath.Join(temp, filename)
		if err := wt.CopyFile(a, b); err != nil {
			return nil, fmt.Errorf("%w: %q", err, filename)
		}
	}

	if err := wt.RecoverToStable(temp); err != nil {
		return nil, fmt.Errorf("recover to stable: %w", err)
	}

	sess, err := wt.OpenSession(temp, wt.BaseConfig+",readonly")
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, temp)
	}
	defer sess.Close()

	metadata, err := wt.ReadCatalog(sess)
	if err != nil {
		return nil, fmt.Errorf("namespaces: %w", err)
	}

	return metadata, nil
}

func doImport(src, dst string, nss []*wt.Namespace) error {
	sess, err := wt.OpenSession(dst, wt.BaseConfig)
	if err != nil {
		return fmt.Errorf("open target session: %w", err)
	}
	defer sess.Close()

	for _, ns := range nss {
		a := filepath.Join(src, ns.Ident+".wt")
		b := filepath.Join(dst, ns.Ident+".wt")
		if err := wt.CopyFile(a, b); err != nil {
			return fmt.Errorf("copy %s.wt: %w", ns.Ident, err)
		}

		for _, ident := range ns.IdxIdent {
			a := filepath.Join(src, ident+".wt")
			b := filepath.Join(dst, ident+".wt")
			if err := wt.CopyFile(a, b); err != nil {
				return fmt.Errorf("copy %s.wt: %w", ident, err)
			}
		}

		if err := wt.ImportCollection(sess, ns); err != nil {
			return fmt.Errorf("import %s: %w", ns.NS, err)
		}
	}

	if err := sess.Checkpoint(); err != nil {
		log.Printf("checkpoint: %v", err)
	}

	return nil
}
