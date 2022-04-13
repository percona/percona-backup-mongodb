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

	srcSet, dstSet := makeStrRegistry(), makeStrRegistry()

	for _, a := range strings.Split(s, ",") {
		m := strings.Split(a, "=")
		if len(m) != 2 {
			return nil, errors.Errorf("malformatted: %q", a)
		}

		src, dst := m[0], m[1]

		if srcSet(src) {
			return nil, errors.Errorf("source %v is duplicated", src)
		}
		if dstSet(dst) {
			return nil, errors.Errorf("target %v is duplicated", dst)
		}

		rv[dst] = src
	}

	return rv, nil
}

func makeStrRegistry() func(string) bool {
	m := make(map[string]struct{})

	return func(s string) bool {
		if _, ok := m[s]; ok {
			return true
		}

		m[s] = struct{}{}
		return false
	}
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

func MakeReverseRSMapFunc(m map[string]string) RSMapFunc {
	return MakeRSMapFunc(swapSSMap(m))
}

func swapSSMap(m map[string]string) map[string]string {
	rv := make(map[string]string, len(m))

	for k, v := range m {
		rv[v] = k
	}

	return rv
}
