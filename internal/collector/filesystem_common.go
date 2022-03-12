package collector

import (
	"bufio"
	"fmt"
	"github.com/lesovsky/pgscv/internal/log"
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
func parseProcMounts(r io.Reader) ([]mount, error) {
	log.Debug("parse mounted filesystems")
	var (
		scanner = bufio.NewScanner(r)
		mounts  []mount
	)

	// Parse line by line, split line to param and value, parse the value to float and save to store.
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())

		if len(parts) != 6 {
			return nil, fmt.Errorf("invalid input: '%s', skip", scanner.Text())
		}

		s := mount{
			device:     parts[0],
			mountpoint: parts[1],
			fstype:     parts[2],
			options:    parts[3],
		}

		mounts = append(mounts, s)
	}

	return mounts, scanner.Err()
}

// truncateDeviceName truncates passed full path to device to short device name.
func truncateDeviceName(path string) string {
	if path == "" {
		log.Warnf("cannot truncate empty device path")
		return ""
	}
	// Make name which will be returned in case of later errors occurred.
	parts := strings.Split(path, "/")
	name := parts[len(parts)-1]

	// Check device path exists.
	fi, err := os.Lstat(path)
	if err != nil {
		log.Debugf("%s, use default '%s'", err, name)
		return name
	}

	// If path is symlink, try dereference it.
	if fi.Mode()&os.ModeSymlink != 0 {
		resolved, err := os.Readlink(path)
		if err != nil {
			log.Warnf("%s, use name's last part '%s'", err, name)
			return name
		}
		// Swap name to dereferenced origin.
		parts := strings.Split(resolved, "/")
		name = parts[len(parts)-1]
	}

	// Return default (or dereferenced) name.
	return name
}
