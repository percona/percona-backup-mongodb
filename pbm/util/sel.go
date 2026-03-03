package util

import (
	"encoding/hex"
	"slices"
	"strings"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

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

// ParseNS breaks namespace into database and collection parts
func ParseNS(ns string) (string, string) {
	db, coll, _ := strings.Cut(ns, ".")

	if db == "*" {
		db = ""
	}
	if coll == "*" {
		coll = ""
	}

	return db, coll
}

// ContainsColl inspects if collection is explicitly specified by name
// within the namespace
func ContainsColl(ns string) bool {
	_, c := ParseNS(ns)
	return c != ""
}

// ContainsSpecifiedColl inspects if any collection exists for multi-ns
func ContainsSpecifiedColl(nss []string) bool {
	return slices.ContainsFunc(nss, ContainsColl)
}

func ValidateUsersAndRolesOpt(usersAndRoles bool, nss []string) error {
	if !IsSelective(nss) && usersAndRoles {
		return errors.New("Including users and roles are only allowed for selected database " +
			"(use --ns flag for selective backup)")
	}
	if len(nss) >= 1 && ContainsSpecifiedColl(nss) && usersAndRoles {
		return errors.New("Including users and roles are not allowed for specific collection. " +
			"Use --ns='db.*' to specify the whole database instead.")
	}

	return nil
}

func MakeSelectedPred(nss []string) archive.NSFilterFn {
	if len(nss) == 0 {
		return archive.DefaultNSFilter
	}

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

// MakeDBMatchFilter builds a bson filter for the "db" field based on the
// selected namespaces. Returns an empty filter if not selective.
// Otherwise, returns a filter matching only the specific databases from
// the namespace list.
func MakeDBMatchFilter(namespaces []string) bson.M {
	if !IsSelective(namespaces) {
		return bson.M{}
	}

	dbs := make(map[string]bool)
	for _, ns := range namespaces {
		db, _ := ParseNS(ns)
		if db == "" {
			return bson.M{}
		}
		dbs[db] = true
	}

	list := make([]string, 0, len(dbs))
	for db := range dbs {
		list = append(list, db)
	}

	return bson.M{"db": bson.M{"$in": list}}
}

type ChunkSelector interface {
	Add(bson.Raw)
	Selected(bson.Raw) bool

	BuildFilter() bson.D
}

type nsChunkMap map[string]struct{}

func NewNSChunkSelector() nsChunkMap {
	return make(nsChunkMap)
}

func (s nsChunkMap) Add(d bson.Raw) {
	ns := d.Lookup("_id").StringValue()
	s[ns] = struct{}{}
}

func (s nsChunkMap) Selected(d bson.Raw) bool {
	ns := d.Lookup("ns").StringValue()
	_, ok := s[ns]
	return ok
}

func (s nsChunkMap) BuildFilter() bson.D {
	nss := make([]string, 0, len(s))
	for ns := range s {
		nss = append(nss, ns)
	}

	return bson.D{{"ns", bson.M{"$in": nss}}}
}

type uuidChunkMap map[string]struct{}

func NewUUIDChunkSelector() uuidChunkMap {
	return make(uuidChunkMap)
}

func (s uuidChunkMap) Add(d bson.Raw) {
	_, data := d.Lookup("uuid").Binary()
	s[hex.EncodeToString(data)] = struct{}{}
}

func (s uuidChunkMap) Selected(d bson.Raw) bool {
	_, data := d.Lookup("uuid").Binary()
	_, ok := s[hex.EncodeToString(data)]
	return ok
}

func (s uuidChunkMap) BuildFilter() bson.D {
	uuids := make([]primitive.Binary, 0, len(s))
	for ns := range s {
		data, _ := hex.DecodeString(ns)
		uuids = append(uuids, primitive.Binary{Subtype: 0x4, Data: data})
	}

	return bson.D{{"uuid", bson.M{"$in": uuids}}}
}
