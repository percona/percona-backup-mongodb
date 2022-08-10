package version

import (
	"testing"
)

func TestCompatibility(t *testing.T) {
	breaking := []string{
		"1.5.0",
		"1.9.0",
		"2.0.0",
	}
	cases := []struct {
		v1         string
		v2         string
		compatible bool
	}{
		{
			"v1.5.5",
			"1.4.1",
			false,
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
		{
			"1.9.0",
			"1.10.0",
			true,
		},
		{
			"1.9.0",
			"2.0.0",
			false,
		},
		{
			"2.0.0",
			"2.1.0",
			true,
		},
		{
			"2.0.0",
			"3.0.0",
			true,
		},
	}

	for _, test := range cases {
		c := compatible(test.v1, test.v2, breaking)
		if c != test.compatible {
			t.Errorf("compatibility of %s & %s should be %v, got %v", test.v1, test.v2, test.compatible, c)
		}
	}
}
