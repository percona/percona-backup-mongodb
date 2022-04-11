package pbm

import (
	"strings"

	"github.com/pkg/errors"
)

func ParseRSNamesMapping(s string) (map[string]string, error) {
	rv := make(map[string]string)

	if s == "" {
		return rv, nil
	}

	for _, a := range strings.Split(s, ",") {
		m := strings.Split(a, "=")
		if len(m) != 2 {
			return nil, errors.Errorf("malformatted: %q", a)
		}

		rv[m[0]] = m[1]
	}

	return rv, nil
}

type RSMapFunc func(string) string

func MakeRSMapFunc(m map[string]string) RSMapFunc {
	return func(s string) string {
		if m != nil {
			if a, ok := m[s]; ok {
				return a
			}
		}

		return s
	}
}
