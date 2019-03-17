// Package stat is used for retrieving different kind of statistics.
// sysctl.go is related to sysctl settings
package stat

import (
	"io/ioutil"
	"path"
	"strconv"
	"strings"
)

const (
	procSysctlBase = "/proc/sys"
)

// GetSysctl read sysctl value from local 'procfs' filesystem
func GetSysctl(sysctl string) (int, error) {
	data, err := ioutil.ReadFile(path.Join(procSysctlBase, strings.Replace(sysctl, ".", "/", -1)))
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
	return ioutil.WriteFile(path.Join(procSysctlBase, sysctl), []byte(strconv.Itoa(new)), 0640)
}
