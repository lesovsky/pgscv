package app

// Contains returns true if array of strings contains specific string
func Contains(ss []string, str string) bool {
	for _, s := range ss {
		if str == s {
			return true
		}
	}
	return false
}
