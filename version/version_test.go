package version_test

import (
	"testing"

	"github.com/percona/percona-backup-mongodb/version"
)

func TestCompatibility(t *testing.T) {
	breaking := []string{
		"1.5.0",
		"1.9.0",
	}
	cases := []struct {
		v1         string
		v2         string
		compatible bool
	}{
		{
			"v1.5.5",
			"1.5.1",
			true,
		},
		{
			"1.4.0",
			"v1.5.4",
			false,
		},
		{
			"1.3.0",
			"1.4.0",
			true,
		},
		{
			"1.4.3",
			"1.3.0",
			true,
		},
		{
			"1.5.0",
			"1.4.2",
			false,
		},
		{
			"1.5.0",
			"1.6.0",
			true,
		},
		{
			"1.8.5",
			"1.5.0",
			true,
		},
		{
			"1.5.5",
			"1.9.0",
			false,
		},
		{
			"1.9.0",
			"1.5.5",
			false,
		},
		{
			"1.4.5",
			"1.9.0",
			false,
		},
		{
			"1.14.5",
			"1.9.0",
			true,
		},
		{
			"1.4.5",
			"1.14.0",
			false,
		},
	}

	for _, test := range cases {
		v := version.NewVersion(version.Info{Version: test.v1}, breaking)
		c := v.CompatibleWith(test.v2)
		if c != test.compatible {
			t.Errorf("compatibility of %s & %s should be %v, got %v", test.v1, test.v2, test.compatible, c)
		}
	}
}
