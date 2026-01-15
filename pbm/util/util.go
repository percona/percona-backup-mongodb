package util

import "fmt"

func Ref[T any](v T) *T {
	return &v
}

func LogProfileArg(p string) string {
	if p == "" {
		return ""
	}
	return fmt.Sprintf("(profile: %q)", p)
}
