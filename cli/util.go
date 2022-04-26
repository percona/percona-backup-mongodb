package cli

import (
	"strings"

	"github.com/pkg/errors"
)

func parseRSNamesMapping(s string) (map[string]string, error) {
	rv := make(map[string]string)

	if s == "" {
		return rv, nil
	}

	checkSrc, checkDst := makeStrSet(), makeStrSet()

	for _, a := range strings.Split(s, ",") {
		m := strings.Split(a, "=")
		if len(m) != 2 {
			return nil, errors.Errorf("malformatted: %q", a)
		}

		src, dst := m[0], m[1]

		if checkSrc(src) {
			return nil, errors.Errorf("source %v is duplicated", src)
		}
		if checkDst(dst) {
			return nil, errors.Errorf("target %v is duplicated", dst)
		}

		rv[dst] = src
	}

	return rv, nil
}

func makeStrSet() func(string) bool {
	m := make(map[string]struct{})

	return func(s string) bool {
		if _, ok := m[s]; ok {
			return true
		}

		m[s] = struct{}{}
		return false
	}
}
