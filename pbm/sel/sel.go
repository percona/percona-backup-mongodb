package sel

import (
	"strings"

	"github.com/percona/percona-backup-mongodb/pbm/archive"
)

func IsSelective(ids []string) bool {
	for _, ns := range ids {
		if ns != "" && ns != "*.*" {
			return true
		}
	}

	return false
}

func MakeSelectedPred(nss []string) archive.NSFilterFn {
	m := make(map[string]map[string]bool)

	for _, ns := range nss {
		db, coll, _ := strings.Cut(ns, ".")
		if db == "*" {
			db = ""
		}
		if coll == "*" {
			coll = ""
		}

		if m[db] == nil {
			m[db] = make(map[string]bool)
		}
		if !m[db][coll] {
			m[db][coll] = true
		}
	}

	return func(ns string) bool {
		db, coll, ok := strings.Cut(ns, ".")
		return (m[""] != nil || m[db][""]) || (ok && m[db][coll])
	}
}
