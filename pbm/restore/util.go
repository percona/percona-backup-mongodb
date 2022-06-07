package restore

func sliceHasString(ss []string, s string) bool {
	for _, e := range ss {
		if e == s {
			return true
		}
	}

	return false
}
