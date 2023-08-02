package wt

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
)

const BaseConfig = "config_base=false"

var BackupFiles = []string{
	// "storage.bson",
	"WiredTiger",
	"WiredTiger.backup",
	"WiredTigerHS.wt",
	"_mdb_catalog.wt",
	"sizeStorer.wt",
}

type Ident = string

type Namespace struct {
	NS       string           `json:"ns"`
	Ident    Ident            `json:"ident"`
	IdxIdent map[string]Ident `json:"idxIdent,omitempty"`

	CollSize `json:"-"`

	rawValue []byte
}

type CollSize struct {
	NumRecords int64 `json:"numRecords"`
	DataSize   int64 `json:"dataSize"`

	rawKey   []byte
	rawValue []byte
}

func CopyFile(src, dst string) error {
	rdr, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer rdr.Close()

	file, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	defer file.Close()

	if _, err := io.Copy(file, rdr); err != nil {
		return fmt.Errorf("copy: %w", err)
	}

	return nil
}

func RecoverToStable(home string) error {
	sess, err := OpenSession(home, BaseConfig)
	if err != nil {
		return err
	}

	return sess.Close()
}

func ReadCatalog(sess *WTSession) ([]*Namespace, error) {
	ss, err := readSizeStorer(sess)
	if err != nil {
		return nil, fmt.Errorf("sizeStorer: %w", err)
	}

	cur, err := sess.OpenCursor("table:_mdb_catalog")
	if err != nil {
		return nil, err
	}

	nss := []*Namespace{}
	for err = cur.Next(); err == nil; err = cur.Next() {
		val, err := cur.Value()
		if err != nil {
			return nil, err
		}

		md := &Namespace{
			rawValue: val.Bytes(),
		}

		if err := bson.Unmarshal(md.rawValue, &md); err != nil {
			return nil, fmt.Errorf("unmarshal: %w", err)
		}

		md.CollSize = ss[md.Ident]
		nss = append(nss, md)
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	return nss, nil
}

func readSizeStorer(sess *WTSession) (map[Ident]CollSize, error) {
	cur, err := sess.OpenCursor("table:sizeStorer")
	if err != nil {
		return nil, err
	}

	rv := make(map[Ident]CollSize)
	for err = cur.Next(); err == nil; err = cur.Next() {
		key, err := cur.Key()
		if err != nil {
			return nil, err
		}
		val, err := cur.Value()
		if err != nil {
			return nil, err
		}
		cs := CollSize{
			rawKey:   key.Bytes(),
			rawValue: val.Bytes(),
		}

		if err := bson.Unmarshal(cs.rawValue, &cs); err != nil {
			return nil, err
		}

		ident := cutString(key.String(), "table:", "\xaf+")
		rv[ident] = cs
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	return rv, nil
}

func cutString(s, prefix, suffix string) string {
	if strings.HasPrefix(s, prefix) {
		s = s[len(prefix):]
	}
	if strings.HasSuffix(s, prefix) {
		s = s[:len(s)-len(suffix)]
	}

	return s
}

func ImportCollection(sess *WTSession, ns *Namespace) error {
	if err := importCatalogRecord(sess, ns); err != nil {
		return fmt.Errorf("catalog: %w", err)
	}
	if err := importSizeRecord(sess, &ns.CollSize); err != nil {
		return fmt.Errorf("sizeStorer: %w", err)
	}
	if err := importWTMetadata(sess, ns); err != nil {
		return fmt.Errorf("metadata: %w", err)
	}

	return nil
}

func importCatalogRecord(sess *WTSession, ns *Namespace) error {
	cur, err := sess.OpenCursor("table:_mdb_catalog")
	if err != nil {
		return err
	}
	defer cur.Close()

	key, err := cur.LargestKey()
	if err != nil {
		return err
	}

	n, err := key.Int64(sess)
	if err != nil {
		return err
	}

	key, err = NewInt64Item(sess, n+1)
	if err != nil {
		return err
	}
	defer key.Drop()

	val := NewItem(ns.rawValue)
	defer val.Drop()

	return cur.Insert(key, val)
}

func importSizeRecord(sess *WTSession, cs *CollSize) error {
	cur, err := sess.OpenCursor("table:sizeStorer")
	if err != nil {
		return err
	}
	defer cur.Close()

	key := NewItem(cs.rawKey)
	defer key.Drop()
	val := NewItem(cs.rawValue)
	defer val.Drop()

	return cur.Insert(key, val)
}

func importWTMetadata(sess *WTSession, ns *Namespace) error {
	if err := sess.importTable(ns.Ident); err != nil {
		return fmt.Errorf("import table:%s: %w", ns.Ident, err)
	}
	for _, ident := range ns.IdxIdent {
		if err := sess.importTable(ident); err != nil {
			return fmt.Errorf("import table:%s: %w", ns.Ident, err)
		}
	}

	return nil
}
