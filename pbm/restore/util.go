package restore

func Contains[T comparable](ss []T, s T) bool {
	for _, e := range ss {
		if e == s {
			return true
		}
	}

	return false
}
