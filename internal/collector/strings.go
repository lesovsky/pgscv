package collector

import (
	"fmt"
	"strconv"
	"strings"
)

// stringsContains returns true if array of strings contains specific string
func stringsContains(ss []string, s string) bool {
	for _, val := range ss {
		if val == s {
			return true
		}
	}
	return false
}

// semverStringToInt parse valid semver version string and returns numeric representation.
func semverStringToInt(version string) (int, error) {
	// remove additional suffix in patch version if exists.
	version = strings.TrimSuffix(version, strings.TrimLeft(version, "1234567890."))

	nums := strings.Split(version, ".")
	if len(nums) < 3 {
		return 0, fmt.Errorf("invalid version string: '%s'", version)
	}

	var res string
	for i, num := range nums {
		if i > 2 {
			break
		}

		switch i {
		case 1, 2:
			if len(num) < 2 {
				num = "0" + num
			}
		}
		res = res + num
	}

	v, err := strconv.Atoi(res)
	if err != nil {
		return 0, err
	}

	return v, nil
}
