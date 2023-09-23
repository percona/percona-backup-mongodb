package util_test

import (
	"reflect"
	"testing"

	"github.com/percona/percona-backup-mongodb/internal/util"
)

func TestSelectedPred(t *testing.T) {
	nss := []string{
		"db0",
		"db0.c0",
		"db0.c1",
		"db1.",
		"db1.c0",
		"db1.c1",
	}
	cases := []struct {
		s []string
		r []string
	}{
		{[]string{"*.*"}, nss},
		{[]string{"db0.*"}, []string{"db0", "db0.c0", "db0.c1"}},
		{[]string{"db1.*"}, []string{"db1.", "db1.c0", "db1.c1"}},
		{[]string{"db1.c1"}, []string{"db1.c1"}},
		{[]string{"db0.c1", "db1.c0"}, []string{"db0.c1", "db1.c0"}},
		{[]string{"db0.c1", "db1.*"}, []string{"db0.c1", "db1.", "db1.c0", "db1.c1"}},
		{[]string{"db0.c2"}, []string{}},
		{[]string{"db2.c0"}, []string{}},
		{[]string{"db2.*"}, []string{}},
	}

	for _, c := range cases {
		s := util.MakeSelectedPred(c.s)
		r := []string{}
		for _, ns := range nss {
			if s(ns) {
				r = append(r, ns)
			}
		}

		if !reflect.DeepEqual(r, c.r) {
			t.Errorf("expected: %v, got: %v", c.r, r)
		}
	}
}
