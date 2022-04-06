package cli

func makeSSFunctor(m map[string]string) func(string) string {
	return func(s string) string {
		if m != nil {
			if a, ok := m[s]; ok {
				return a
			}
		}

		return s
	}
}
