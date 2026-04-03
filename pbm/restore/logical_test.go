package restore

import (
	"context"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	pbmlog "github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/snapshot"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
)

func TestResolveNamespace(t *testing.T) {
	checkNS := func(t testing.TB, got, want []string) {
		t.Helper()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got=%v, want=%v", got, want)
		}
	}

	t.Run("without users&roles", func(t *testing.T) {
		testCases := []struct {
			desc               string
			nssBackup          []string
			nssRestore         []string
			userAndRolesBackup bool // doesn't apply to full backups
			want               []string
		}{
			{
				desc:       "full backup -> full restore",
				nssBackup:  []string{},
				nssRestore: []string{},
				want:       []string{},
			},
			{
				desc:       "full backup -> selective restore",
				nssBackup:  []string{},
				nssRestore: []string{"d.c"},
				want:       []string{"d.c"},
			},
			{
				desc:               "selective backup (no usersAndRoles) -> full restore",
				nssBackup:          []string{"d.c"},
				nssRestore:         []string{},
				userAndRolesBackup: false,
				want:               []string{"d.c"},
			},
			{
				desc:               "selective backup (usersAndRoles) -> full restore",
				nssBackup:          []string{"d.c"},
				nssRestore:         []string{},
				userAndRolesBackup: true,
				want:               []string{"d.c", "admin.pbmRUsers", "admin.pbmRRoles"},
			},
			{
				desc:               "selective backup (no usersAndRoles)-> selective restore",
				nssBackup:          []string{"d.*"},
				nssRestore:         []string{"d.c"},
				userAndRolesBackup: false,
				want:               []string{"d.c"},
			},
			{
				desc:               "selective backup (usersAndRoles)-> selective restore",
				nssBackup:          []string{"d.*"},
				nssRestore:         []string{"d.c"},
				userAndRolesBackup: true,
				want:               []string{"d.c"},
			},
		}
		for _, tC := range testCases {
			t.Run(tC.desc, func(t *testing.T) {
				got := resolveNamespace(
					tC.nssBackup,
					tC.nssRestore,
					snapshot.CloneNS{},
					tC.userAndRolesBackup,
					false,
				)
				checkNS(t, got, tC.want)
			})
		}
	})

	t.Run("with users&roles", func(t *testing.T) {
		testCases := []struct {
			desc               string
			nssBackup          []string
			nssRestore         []string
			userAndRolesBackup bool // doesn't apply to full backups
			want               []string
		}{
			{
				desc:       "full backup -> full restore",
				nssBackup:  []string{},
				nssRestore: []string{},
				want:       []string{},
			},
			{
				desc:       "full backup -> selective restore",
				nssBackup:  []string{},
				nssRestore: []string{"d.c"},
				want:       []string{"d.c", "admin.pbmRUsers", "admin.pbmRRoles"},
			},
			{
				desc:               "selective backup (no usersAndRoles) -> full restore",
				nssBackup:          []string{"d.c"},
				nssRestore:         []string{},
				userAndRolesBackup: false,
				want:               []string{"d.c"},
			},
			{
				desc:               "selective backup (usersAndRoles) -> full restore",
				nssBackup:          []string{"d.c"},
				nssRestore:         []string{},
				userAndRolesBackup: true,
				want:               []string{"d.c", "admin.pbmRUsers", "admin.pbmRRoles"},
			},
		}
		for _, tC := range testCases {
			t.Run(tC.desc, func(t *testing.T) {
				got := resolveNamespace(
					tC.nssBackup,
					tC.nssRestore,
					snapshot.CloneNS{},
					tC.userAndRolesBackup,
					true,
				)
				checkNS(t, got, tC.want)
			})
		}
	})

	t.Run("cloning collection", func(t *testing.T) {
		testCases := []struct {
			desc                string
			nssBackup           []string
			nsFrom              string
			nsTo                string
			usersAndRolesBackup bool // doesn't apply to full backups
			userAndRoles        bool
			want                []string
		}{
			{
				desc:      "full backup -> cloning collection",
				nssBackup: []string{},
				nsFrom:    "d.from",
				nsTo:      "d.to",
				want:      []string{"d.from"},
			},
			{
				desc:                "selective backup (no usersAndRoles) -> cloning collection",
				nssBackup:           []string{"d.*", "a.b"},
				nsFrom:              "d.from",
				nsTo:                "d.to",
				usersAndRolesBackup: false,
				userAndRoles:        false,
				want:                []string{"d.from"},
			},
			{
				desc:                "selective backup (no usersAndRoles) -> cloning collection",
				nssBackup:           []string{"d.*", "a.b"},
				nsFrom:              "d.from",
				nsTo:                "d.to",
				usersAndRolesBackup: true,
				userAndRoles:        false,
				want:                []string{"d.from"},
			},
			{
				desc:         "full backup -> cloning collection, users&roles are ignored",
				nssBackup:    []string{},
				nsFrom:       "d.from",
				nsTo:         "d.to",
				userAndRoles: true,
				want:         []string{"d.from"},
			},
			{
				desc:                "selective backup (no usersAndRoles) -> cloning collection, users&roles are ignored",
				nssBackup:           []string{"d.*", "a.b"},
				nsFrom:              "d.from",
				nsTo:                "d.to",
				usersAndRolesBackup: false,
				userAndRoles:        true,
				want:                []string{"d.from"},
			},
			{
				desc:                "selective backup (usersAndRoles) -> cloning collection, users&roles are ignored",
				nssBackup:           []string{"d.*", "a.b"},
				nsFrom:              "d.from",
				nsTo:                "d.to",
				usersAndRolesBackup: true,
				userAndRoles:        true,
				want:                []string{"d.from"},
			},
		}
		for _, tC := range testCases {
			t.Run(tC.desc, func(t *testing.T) {
				got := resolveNamespace(
					tC.nssBackup,
					[]string{},
					snapshot.CloneNS{FromNS: tC.nsFrom, ToNS: tC.nsTo},
					tC.usersAndRolesBackup,
					tC.userAndRoles)
				checkNS(t, got, tC.want)
			})
		}
	})
}

func TestShouldRestoreUsersAndRoles(t *testing.T) {
	checkOpt := func(t testing.TB, got, want restoreUsersAndRolesOption) {
		t.Helper()
		if got != want {
			t.Errorf("invalid restoreUsersAndRolesOption got=%t, want=%t", got, want)
		}
	}

	t.Run("without users&roles", func(t *testing.T) {
		testCases := []struct {
			desc               string
			nssBackup          []string
			nssRestore         []string
			nsFrom             string
			nsTo               string
			userAndRolesBackup bool // doesn't apply to full backups
			wantOpt            restoreUsersAndRolesOption
		}{
			{
				desc:       "full backup -> full restore",
				nssBackup:  []string{},
				nssRestore: []string{},
				wantOpt:    true,
			},
			{
				desc:       "full backup -> selective restore",
				nssBackup:  []string{},
				nssRestore: []string{"d.c"},
				wantOpt:    false,
			},
			{
				desc:               "selective backup (no usersAndRoles) -> full restore",
				nssBackup:          []string{"d.c"},
				nssRestore:         []string{},
				userAndRolesBackup: false,
				wantOpt:            false,
			},
			{
				desc:               "selective backup (usersAndRoles) -> full restore",
				nssBackup:          []string{"d.c"},
				nssRestore:         []string{},
				userAndRolesBackup: true,
				wantOpt:            true,
			},
			{
				desc:       "full backup -> cloning collection",
				nssBackup:  []string{},
				nssRestore: []string{},
				nsFrom:     "d.from",
				nsTo:       "d.to",
				wantOpt:    false,
			},
			{
				desc:       "selective backup -> cloning collection",
				nssBackup:  []string{"d.*"},
				nssRestore: []string{},
				nsFrom:     "d.from",
				nsTo:       "d.to",
				wantOpt:    false,
			},
		}
		for _, tC := range testCases {
			t.Run(tC.desc, func(t *testing.T) {
				gotOpt := shouldRestoreUsersAndRoles(
					tC.nssBackup,
					tC.nssRestore,
					snapshot.CloneNS{FromNS: tC.nsFrom, ToNS: tC.nsTo},
					tC.userAndRolesBackup,
					false)
				checkOpt(t, gotOpt, tC.wantOpt)
			})
		}
	})

	t.Run("with users&roles", func(t *testing.T) {
		testCases := []struct {
			desc               string
			nssBackup          []string
			nssRestore         []string
			userAndRolesBackup bool // doesn't apply to full backups
			wantOpt            restoreUsersAndRolesOption
		}{
			{
				desc:       "full backup -> full restore",
				nssBackup:  []string{},
				nssRestore: []string{},
				wantOpt:    true,
			},
			{
				desc:       "full backup -> selective restore",
				nssBackup:  []string{},
				nssRestore: []string{"d.c"},
				wantOpt:    true,
			},
			{
				desc:               "selective backup (no usersAndRoles) -> full restore",
				nssBackup:          []string{"d.c"},
				nssRestore:         []string{},
				userAndRolesBackup: false,
				wantOpt:            false,
			},
			{
				desc:               "selective backup (usersAndRoles) -> full restore",
				nssBackup:          []string{"d.c"},
				nssRestore:         []string{},
				userAndRolesBackup: true,
				wantOpt:            true,
			},
		}
		for _, tC := range testCases {
			t.Run(tC.desc, func(t *testing.T) {
				gotOpt := shouldRestoreUsersAndRoles(
					tC.nssBackup,
					tC.nssRestore,
					snapshot.CloneNS{},
					tC.userAndRolesBackup,
					true,
				)
				checkOpt(t, gotOpt, tC.wantOpt)
			})
		}
	})
}

func TestSelRestoreDBCleanup(t *testing.T) {
	ctx := context.Background()

	t.Run("drop single database", func(t *testing.T) {
		r, db := createCleanupRestoreTest(t, "rs0")

		err := createConfigDatabasesDoc(t, "db", "rs0")
		if err != nil {
			t.Fatalf("create config.databases doc: %v", err)
		}

		nss := []string{"db.*"}
		err = r.selRestoreDBCleanup(ctx, nss)
		if err != nil {
			t.Fatalf("selRestoreDBCleanup: %v", err)
		}

		if len(db.dropDatabaseCalls) != 1 {
			t.Fatalf("expected 1 dropDatabase call, got %d", len(db.dropDatabaseCalls))
		}
		if db.dropDatabaseCalls[0] != "db" {
			t.Fatalf("expected dropDatabase to be called with %q, got %q", "db", db.dropDatabaseCalls[0])
		}
		if len(db.dropCollectionCalls) != 0 {
			t.Fatalf("expected 0 dropCollection calls, got %d", len(db.dropCollectionCalls))
		}
	})

	t.Run("drop single collection", func(t *testing.T) {
		r, db := createCleanupRestoreTest(t, "rs0")

		err := createConfigDatabasesDoc(t, "db", "rs0")
		if err != nil {
			t.Fatalf("create config.databases doc: %v", err)
		}

		nss := []string{"db.coll"}
		err = r.selRestoreDBCleanup(ctx, nss)
		if err != nil {
			t.Fatalf("selRestoreDBCleanup: %v", err)
		}

		if len(db.dropCollectionCalls) != 1 {
			t.Fatalf("expected 1 dropCollection call, got %d", len(db.dropCollectionCalls))
		}
		if db.dropCollectionCalls[0].db != "db" {
			t.Fatalf("expected dropCollection db to be %q, got %q", "db", db.dropCollectionCalls[0].db)
		}
		if db.dropCollectionCalls[0].coll != "coll" {
			t.Fatalf("expected dropCollection coll to be %q, got %q", "coll", db.dropCollectionCalls[0].coll)
		}
		if len(db.dropDatabaseCalls) != 0 {
			t.Fatalf("expected 0 dropDatabase calls, got %d", len(db.dropDatabaseCalls))
		}
	})

	t.Run("drop multiple databases", func(t *testing.T) {
		r, db := createCleanupRestoreTest(t, "rs0")

		err := createConfigDatabasesDoc(t, "db1", "rs0")
		if err != nil {
			t.Fatalf("create config.databases doc 1: %v", err)
		}
		err = createConfigDatabasesDoc(t, "db2", "rs0")
		if err != nil {
			t.Fatalf("create config.databases doc 2: %v", err)
		}

		nss := []string{"db1.*", "db2.*"}
		err = r.selRestoreDBCleanup(ctx, nss)
		if err != nil {
			t.Fatalf("selRestoreDBCleanup: %v", err)
		}

		if len(db.dropDatabaseCalls) != 2 {
			t.Fatalf("expected 2 dropDatabase calls, got %d", len(db.dropDatabaseCalls))
		}
		if db.dropDatabaseCalls[0] != "db1" {
			t.Fatalf("expected first dropDatabase call with %q, got %q", "db1", db.dropDatabaseCalls[0])
		}
		if db.dropDatabaseCalls[1] != "db2" {
			t.Fatalf("expected second dropDatabase call with %q, got %q", "db2", db.dropDatabaseCalls[1])
		}
		if len(db.dropCollectionCalls) != 0 {
			t.Fatalf("expected 0 dropCollection calls, got %d", len(db.dropCollectionCalls))
		}
	})

	t.Run("drop multiple databases and collections", func(t *testing.T) {
		r, db := createCleanupRestoreTest(t, "rs0")

		err := createConfigDatabasesDoc(t, "db1", "rs0")
		if err != nil {
			t.Fatalf("create config.databases doc 1: %v", err)
		}
		err = createConfigDatabasesDoc(t, "db2", "rs0")
		if err != nil {
			t.Fatalf("create config.databases doc 2: %v", err)
		}

		nss := []string{"db1.c1", "db2.c2", "db2.c3", "db1.*"}
		err = r.selRestoreDBCleanup(ctx, nss)
		if err != nil {
			t.Fatalf("selRestoreDBCleanup: %v", err)
		}

		if len(db.dropDatabaseCalls) != 1 {
			t.Fatalf("expected 1 dropDatabase call, got %d", len(db.dropDatabaseCalls))
		}
		if db.dropDatabaseCalls[0] != "db1" {
			t.Fatalf("expected first dropDatabase call with %q, got %q", "db1", db.dropDatabaseCalls[0])
		}
		if len(db.dropCollectionCalls) != 3 {
			t.Fatalf("expected 3 dropCollection calls, got %d", len(db.dropCollectionCalls))
		}
		if db.dropCollectionCalls[0].coll != "c1" {
			t.Fatalf("expected dropCollection coll to be c1, got %q", db.dropCollectionCalls[0].coll)
		}
		if db.dropCollectionCalls[1].coll != "c2" {
			t.Fatalf("expected dropCollection coll to be c2, got %q", db.dropCollectionCalls[1].coll)
		}
		if db.dropCollectionCalls[2].coll != "c3" {
			t.Fatalf("expected dropCollection coll to be c3, got %q", db.dropCollectionCalls[2].coll)
		}
	})

	t.Run("skip admin and config databases", func(t *testing.T) {
		r, db := createCleanupRestoreTest(t, "rs0")

		nss := []string{defs.DB + ".*", defs.ConfigDB + ".*"}
		err := r.selRestoreDBCleanup(ctx, nss)
		if err != nil {
			t.Fatalf("selRestoreDBCleanup: %v", err)
		}

		if len(db.dropDatabaseCalls) != 0 {
			t.Fatalf("expected 0 dropDatabase calls, got %d", len(db.dropDatabaseCalls))
		}
		if len(db.dropCollectionCalls) != 0 {
			t.Fatalf("expected 0 dropCollection calls, got %d", len(db.dropCollectionCalls))
		}
	})

	t.Run("skip database not in config.databases", func(t *testing.T) {
		r, db := createCleanupRestoreTest(t, "rs0")

		nss := []string{"nonexistent.*"}
		err := r.selRestoreDBCleanup(ctx, nss)
		if err != nil {
			t.Fatalf("selRestoreDBCleanup: %v", err)
		}

		if len(db.dropDatabaseCalls) != 0 {
			t.Fatalf("expected 0 dropDatabase calls, got %d", len(db.dropDatabaseCalls))
		}
		if len(db.dropCollectionCalls) != 0 {
			t.Fatalf("expected 0 dropCollection calls, got %d", len(db.dropCollectionCalls))
		}
	})

	t.Run("skip database with different primary shard", func(t *testing.T) {
		r, db := createCleanupRestoreTest(t, "rs0")

		err := createConfigDatabasesDoc(t, "db", "rs1")
		if err != nil {
			t.Fatalf("create config.databases doc: %v", err)
		}

		nss := []string{"db.*"}
		err = r.selRestoreDBCleanup(ctx, nss)
		if err != nil {
			t.Fatalf("selRestoreDBCleanup: %v", err)
		}

		if len(db.dropDatabaseCalls) != 0 {
			t.Fatalf("expected 0 dropDatabase calls, got %d", len(db.dropDatabaseCalls))
		}
		if len(db.dropCollectionCalls) != 0 {
			t.Fatalf("expected 0 dropCollection calls, got %d", len(db.dropCollectionCalls))
		}
	})

	t.Run("skip collection with different primary shard", func(t *testing.T) {
		r, db := createCleanupRestoreTest(t, "rs0")

		err := createConfigDatabasesDoc(t, "db", "rs1")
		if err != nil {
			t.Fatalf("create config.databases doc: %v", err)
		}

		nss := []string{"db.coll"}
		err = r.selRestoreDBCleanup(ctx, nss)
		if err != nil {
			t.Fatalf("selRestoreDBCleanup: %v", err)
		}

		if len(db.dropDatabaseCalls) != 0 {
			t.Fatalf("expected 0 dropDatabase calls, got %d", len(db.dropDatabaseCalls))
		}
		if len(db.dropCollectionCalls) != 0 {
			t.Fatalf("expected 0 dropCollection calls, got %d", len(db.dropCollectionCalls))
		}
	})

	t.Run("drop database and collection in same namespace list", func(t *testing.T) {
		r, db := createCleanupRestoreTest(t, "rs0")

		err := createConfigDatabasesDoc(t, "db1", "rs0")
		if err != nil {
			t.Fatalf("create config.databases doc 1: %v", err)
		}
		err = createConfigDatabasesDoc(t, "db2", "rs0")
		if err != nil {
			t.Fatalf("create config.databases doc 2: %v", err)
		}

		nss := []string{"db1.*", "db2.coll"}
		err = r.selRestoreDBCleanup(ctx, nss)
		if err != nil {
			t.Fatalf("selRestoreDBCleanup: %v", err)
		}

		if len(db.dropDatabaseCalls) != 1 {
			t.Fatalf("expected 1 dropDatabase call, got %d", len(db.dropDatabaseCalls))
		}
		if db.dropDatabaseCalls[0] != "db1" {
			t.Fatalf("expected dropDatabase call with %q, got %q", "db1", db.dropDatabaseCalls[0])
		}

		if len(db.dropCollectionCalls) != 1 {
			t.Fatalf("expected 1 dropCollection call, got %d", len(db.dropCollectionCalls))
		}
		if db.dropCollectionCalls[0].db != "db2" {
			t.Fatalf("expected dropCollection db to be %q, got %q", "db2", db.dropCollectionCalls[0].db)
		}
		if db.dropCollectionCalls[0].coll != "coll" {
			t.Fatalf("expected dropCollection coll to be %q, got %q", "coll", db.dropCollectionCalls[0].coll)
		}
	})

	t.Run("skip already dropped database", func(t *testing.T) {
		r, db := createCleanupRestoreTest(t, "rs0")

		err := createConfigDatabasesDoc(t, "db", "rs0")
		if err != nil {
			t.Fatalf("create config.databases doc: %v", err)
		}

		nss := []string{"db.*", "db.c"}
		err = r.selRestoreDBCleanup(ctx, nss)
		if err != nil {
			t.Fatalf("selRestoreDBCleanup: %v", err)
		}

		if len(db.dropDatabaseCalls) != 1 {
			t.Fatalf("expected 1 dropDatabase call, got %d", len(db.dropDatabaseCalls))
		}
		if db.dropDatabaseCalls[0] != "db" {
			t.Fatalf("expected dropDatabase call with %q, got %q", "db", db.dropDatabaseCalls[0])
		}
		if len(db.dropCollectionCalls) != 0 {
			t.Fatalf("expected 0 dropCollection calls, got %d", len(db.dropCollectionCalls))
		}
	})

	t.Run("skip already dropped multiple databases", func(t *testing.T) {
		r, db := createCleanupRestoreTest(t, "rs0")

		err := createConfigDatabasesDoc(t, "db1", "rs0")
		if err != nil {
			t.Fatalf("create config.databases doc: %v", err)
		}
		err = createConfigDatabasesDoc(t, "db2", "rs0")
		if err != nil {
			t.Fatalf("create config.databases doc: %v", err)
		}

		nss := []string{"db1.*", "db2.*", "db1.c1", "db1.c2", "db2.c2"}
		err = r.selRestoreDBCleanup(ctx, nss)
		if err != nil {
			t.Fatalf("selRestoreDBCleanup: %v", err)
		}

		if len(db.dropDatabaseCalls) != 2 {
			t.Fatalf("expected 2 dropDatabase calls, got %d", len(db.dropDatabaseCalls))
		}
		if len(db.dropCollectionCalls) != 0 {
			t.Fatalf("expected 0 dropCollection calls, got %d", len(db.dropCollectionCalls))
		}
	})

	t.Run("skip already dropped databases preserves order", func(t *testing.T) {
		r, db := createCleanupRestoreTest(t, "rs0")

		err := createConfigDatabasesDoc(t, "db1", "rs0")
		if err != nil {
			t.Fatalf("create config.databases doc: %v", err)
		}
		err = createConfigDatabasesDoc(t, "db2", "rs0")
		if err != nil {
			t.Fatalf("create config.databases doc: %v", err)
		}

		nss := []string{"db1.c1", "db2.c1", "db1.*", "db2.*", "db1.c2", "db2.c2"}
		err = r.selRestoreDBCleanup(ctx, nss)
		if err != nil {
			t.Fatalf("selRestoreDBCleanup: %v", err)
		}

		if len(db.dropDatabaseCalls) != 2 {
			t.Fatalf("expected 2 dropDatabase calls, got %d", len(db.dropDatabaseCalls))
		}
		if len(db.dropCollectionCalls) != 2 {
			t.Fatalf("expected 2 dropCollection calls, got %d", len(db.dropCollectionCalls))
		}
	})
}

func TestFullRestoreDBCleanup(t *testing.T) {
	ctx := context.Background()

	t.Run("drop single database", func(t *testing.T) {
		r, db := createCleanupRestoreTest(t, "rs0")

		err := createConfigDatabasesDoc(t, "db1", "rs0")
		if err != nil {
			t.Fatalf("create config.databases doc: %v", err)
		}

		bcp := &backup.BackupMeta{Name: "backup1"}
		err = r.fullRestoreDBCleanup(ctx, bcp)
		if err != nil {
			t.Fatalf("fullRestoreDBCleanup: %v", err)
		}

		if len(db.dropDatabaseCalls) != 1 {
			t.Fatalf("expected 1 dropDatabase call, got %d", len(db.dropDatabaseCalls))
		}
		if db.dropDatabaseCalls[0] != "db1" {
			t.Fatalf("expected dropDatabase to be called with %q, got %q", "db", db.dropDatabaseCalls[0])
		}
	})

	t.Run("drop multiple databases", func(t *testing.T) {
		r, db := createCleanupRestoreTest(t, "rs0")

		err := createConfigDatabasesDoc(t, "db1", "rs0")
		if err != nil {
			t.Fatalf("create config.databases doc 1: %v", err)
		}
		err = createConfigDatabasesDoc(t, "db2", "rs0")
		if err != nil {
			t.Fatalf("create config.databases doc 2: %v", err)
		}

		bcp := &backup.BackupMeta{Name: "backup1"}
		err = r.fullRestoreDBCleanup(ctx, bcp)
		if err != nil {
			t.Fatalf("fullRestoreDBCleanup: %v", err)
		}

		if len(db.dropDatabaseCalls) != 2 {
			t.Fatalf("expected 2 dropDatabase calls, got %d", len(db.dropDatabaseCalls))
		}
		if !slices.Contains(db.dropDatabaseCalls, "db1") {
			t.Fatal("expected dropDatabase call for db1")
		}
		if !slices.Contains(db.dropDatabaseCalls, "db2") {
			t.Fatal("expected dropDatabase call for db2")
		}
	})

	t.Run("skip database not in config.databases", func(t *testing.T) {
		r, db := createCleanupRestoreTest(t, "rs0")

		bcp := &backup.BackupMeta{Name: "backup1"}
		err := r.fullRestoreDBCleanup(ctx, bcp)
		if err != nil {
			t.Fatalf("fullRestoreDBCleanup: %v", err)
		}

		if len(db.dropDatabaseCalls) != 0 {
			t.Fatalf("expected 0 dropDatabase calls, got %d", len(db.dropDatabaseCalls))
		}
	})

	t.Run("skip database with different primary shard", func(t *testing.T) {
		r, db := createCleanupRestoreTest(t, "rs0")

		err := createConfigDatabasesDoc(t, "db1", "rs1")
		if err != nil {
			t.Fatalf("create config.databases doc: %v", err)
		}

		bcp := &backup.BackupMeta{Name: "backup1"}
		err = r.fullRestoreDBCleanup(ctx, bcp)
		if err != nil {
			t.Fatalf("fullRestoreDBCleanup: %v", err)
		}

		if len(db.dropDatabaseCalls) != 0 {
			t.Fatalf("expected 0 dropDatabase calls, got %d", len(db.dropDatabaseCalls))
		}
	})
}

func createCleanupRestoreTest(t *testing.T, setName string) (*Restore, *mockMDB) {
	t.Helper()

	nodeConn := leadConn.MongoClient()
	restore := New(
		leadConn,
		nodeConn,
		topo.NodeBrief{
			SetName: setName,
			Sharded: true,
		},
		nil, nil, 1, 1)
	restore.log = pbmlog.DiscardEvent
	restore.nodeInfo = &topo.NodeInfo{
		SetName:           setName,
		ConfigServerState: &topo.ConfigServerState{}, // Makes it sharded
	}

	dbMock := &mockMDB{
		leadConn:          leadConn,
		mDB:               newMDB(leadConn, nodeConn),
		dropDatabaseCalls: []string{},
		dropCollectionCalls: []struct {
			db   string
			coll string
		}{},
	}
	restore.db = dbMock

	restore.bcpStg = &mockBcpStg{}

	t.Cleanup(func() {
		cleanupTestData(t)
	})

	return restore, dbMock
}

func createConfigDatabasesDoc(t *testing.T, dbName, primaryShard string) error {
	t.Helper()

	ctx := context.Background()
	doc := bson.D{
		{"_id", dbName},
		{"primary", primaryShard},
		{"version", bson.D{
			{"uuid", primitive.Binary{Subtype: 0x04, Data: []byte("test-uuid")}},
			{"timestamp", primitive.Timestamp{T: 1, I: 0}},
			{"lastMod", int64(1)},
		}},
	}

	_, err := leadConn.ConfigDatabase().Collection("databases").InsertOne(ctx, doc)
	return err
}

func cleanupTestData(t *testing.T) {
	t.Helper()

	ctx := context.Background()

	_, err := leadConn.ConfigDatabase().Collection("databases").DeleteMany(ctx, bson.D{})
	if err != nil {
		t.Fatalf("clean up config.databases: %v", err)
	}
}

type mockMDB struct {
	leadConn            connect.Client
	mDB                 *mDB
	dropDatabaseCalls   []string
	dropCollectionCalls []struct {
		db   string
		coll string
	}
}

func (m *mockMDB) runCmdShardsvrDropDatabase(
	ctx context.Context,
	db string,
	configDBDoc *configDatabasesDoc,
) error {
	m.dropDatabaseCalls = append(m.dropDatabaseCalls, db)
	return nil
}

func (m *mockMDB) runCmdShardsvrDropCollection(
	ctx context.Context,
	db, coll string,
	configDBDoc *configDatabasesDoc,
) error {
	m.dropCollectionCalls = append(m.dropCollectionCalls, struct {
		db   string
		coll string
	}{db: db, coll: coll})
	return nil
}

func (m *mockMDB) getConfigDatabasesDoc(
	ctx context.Context,
	db string,
) (*configDatabasesDoc, error) {
	return m.mDB.getConfigDatabasesDoc(ctx, db)
}

type mockBcpStg struct{}

func (m *mockBcpStg) Type() storage.Type {
	return storage.Filesystem
}

func (m *mockBcpStg) Save(_ string, _ io.Reader, _ ...storage.Option) error {
	return nil
}

func (m *mockBcpStg) List(_, _ string) ([]storage.FileInfo, error) {
	return nil, nil
}

func (m *mockBcpStg) Delete(_ string) error {
	return nil
}

func (m *mockBcpStg) FileStat(_ string) (storage.FileInfo, error) {
	return storage.FileInfo{}, nil
}

func (m *mockBcpStg) Copy(_, _ string) error {
	return nil
}

func (m *mockBcpStg) DownloadStat() storage.DownloadStat {
	return storage.DownloadStat{}
}

func (m *mockBcpStg) SourceReader(filepath string) (io.ReadCloser, error) {
	metaJson := `
		{
		  "namespaces": [
			{
			  "db": "admin",
			  "collection": "system.version",
			  "metadata": "",
			  "type": "collection",
			  "crc": {
				"$numberLong": "7899326899661778306"
			  },
			  "size": {
				"$numberLong": "272"
			  }
			},
			{
			  "db": "db1",
			  "collection": "c1",
			  "metadata": "",
			  "type": "collection",
			  "crc": {
				"$numberLong": "-7684575339597196538"
			  },
			  "size": {
				"$numberLong": "24795"
			  }
			},
			{
			  "db": "db2",
			  "collection": "c1",
			  "metadata": "",
			  "type": "collection",
			  "crc": {
				"$numberLong": "-7684575339597196538"
			  },
			  "size": {
				"$numberLong": "24795"
			  }
			}
		  ]
		}
	`
	return io.NopCloser(strings.NewReader(metaJson)), nil
}
