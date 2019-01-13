package stat

import (
	"io/ioutil"
	"path"
	"strconv"
	"strings"
)

const (
	PROC_SYSCTL_BASE = "/proc/sys"
)

func GetSysctl(sysctl string) (int, error) {
	data, err := ioutil.ReadFile(path.Join(PROC_SYSCTL_BASE, strings.Replace(sysctl, ".", "/", -1)))
	if err != nil {
		return -1, err
	}
	val, err := strconv.Atoi(strings.Trim(string(data), " \n"))
	if err != nil {
		return -1, err
	}
	return val, nil
}

// SetSysctl modifies the specified sysctl flag to the new value
func SetSysctl(sysctl string, new int) error {
	return ioutil.WriteFile(path.Join(PROC_SYSCTL_BASE, sysctl), []byte(strconv.Itoa(new)), 0640)
}
