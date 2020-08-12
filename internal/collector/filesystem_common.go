package collector

import (
	"bufio"
	"fmt"
	"github.com/barcodepro/pgscv/internal/filter"
	"github.com/barcodepro/pgscv/internal/log"
	"io"
	"os"
	"strings"
)

// mount describes properties of mounted filesystems
type mount struct {
	device     string
	mountpoint string
	fstype     string
	options    string
}

// parseProcMounts parses /proc/mounts and returns slice of mounted filesystems properties.
func parseProcMounts(r io.Reader, filters map[string]filter.Filter) ([]mount, error) {
	var (
		scanner = bufio.NewScanner(r)
		mounts  []mount
	)

	// Parse line by line, split line to param and value, parse the value to float and save to store.
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())

		if len(parts) != 6 {
			return nil, fmt.Errorf("/proc/mounts invalid line: %s; skip", scanner.Text())
		}

		fstype := parts[2]
		if f, ok := filters["filesystem/fstype"]; ok {
			if !f.Pass(fstype) {
				log.Debugln("ignore filesystem ", fstype)
				continue
			}
		}

		s := mount{
			device:     parts[0],
			mountpoint: parts[1],
			fstype:     fstype,
			options:    parts[3],
		}

		mounts = append(mounts, s)
	}

	return mounts, scanner.Err()
}

// truncateDeviceName truncates passed full path to device to short device name.
func truncateDeviceName(path string) (string, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return "", err
	}

	if fi.Mode()&os.ModeSymlink != 0 {
		resolved, err := os.Readlink(path)
		if err != nil {
			return "", err
		}
		path = resolved
	}

	parts := strings.Split(path, "/")

	return parts[len(parts)-1], nil
}
