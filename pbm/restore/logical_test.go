package restore

import (
	"reflect"
	"testing"
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
			desc       string
			nssBackup  []string
			nssRestore []string
			want       []string
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
				desc:       "selective backup -> full restore",
				nssBackup:  []string{"d.c"},
				nssRestore: []string{},
				want:       []string{"d.c"},
			},
			{
				desc:       "selective backup -> selective restore",
				nssBackup:  []string{"d.*"},
				nssRestore: []string{"d.c"},
				want:       []string{"d.c"},
			},
		}
		for _, tC := range testCases {
			t.Run(tC.desc, func(t *testing.T) {
				got := resolveNamespace(tC.nssBackup, tC.nssRestore, "", "", false)
				checkNS(t, got, tC.want)
			})
		}
	})

	t.Run("with users&roles", func(t *testing.T) {
		testCases := []struct {
			desc       string
			nssBackup  []string
			nssRestore []string
			want       []string
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
				desc:       "selective backup -> full restore",
				nssBackup:  []string{"d.c"},
				nssRestore: []string{},
				want:       []string{"d.c"},
			},
		}
		for _, tC := range testCases {
			t.Run(tC.desc, func(t *testing.T) {
				got := resolveNamespace(tC.nssBackup, tC.nssRestore, "", "", true)
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
			desc       string
			nssBackup  []string
			nssRestore []string
			wantOpt    restoreUsersAndRolesOption
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
				desc:       "selective backup -> full restore",
				nssBackup:  []string{"d.c"},
				nssRestore: []string{},
				wantOpt:    false,
			},
		}
		for _, tC := range testCases {
			t.Run(tC.desc, func(t *testing.T) {
				gotOpt := shouldRestoreUsersAndRoles(tC.nssBackup, tC.nssRestore, "", "", false)
				checkOpt(t, gotOpt, tC.wantOpt)
			})
		}
	})

	t.Run("with users&roles", func(t *testing.T) {
		testCases := []struct {
			desc       string
			nssBackup  []string
			nssRestore []string
			wantOpt    restoreUsersAndRolesOption
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
				desc:       "selective backup -> full restore",
				nssBackup:  []string{"d.c"},
				nssRestore: []string{},
				wantOpt:    false,
			},
		}
		for _, tC := range testCases {
			t.Run(tC.desc, func(t *testing.T) {
				gotOpt := shouldRestoreUsersAndRoles(tC.nssBackup, tC.nssRestore, "", "", true)
				checkOpt(t, gotOpt, tC.wantOpt)
			})
		}
	})
}
