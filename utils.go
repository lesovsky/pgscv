package main

// проверка строки на наличие в массиве строк
func Contains(ss []string, str string) bool {
	for _, s := range ss {
		if str == s {
			return true
		}
	}
	return false
}
