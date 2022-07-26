package archive

import (
	"strings"
)

func NSify(db, coll string) string {
	return db + "." + strings.TrimPrefix(coll, "system.buckets.")
}

func FormatNS(db, coll string) string {
	if db == "" {
		return "*.*"
	}
	if coll == "" {
		coll = "*"
	}

	return db + "." + coll
}

func ParseNS(ns string) (string, string) {
	var db, coll string

	switch s := strings.SplitN(ns, ".", 2); len(s) {
	case 0:
		return "", ""
	case 1:
		db = s[0]
	case 2:
		db, coll = s[0], s[1]
	}

	if db == "*" {
		db = ""
	}
	if coll == "*" {
		coll = ""
	}

	return db, coll
}

func FormatFilepath(bcp, rs, filename string) string {
	return bcp + "/" + rs + "/" + filename
}
