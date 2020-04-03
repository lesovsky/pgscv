// Package stat is used for retrieving different kind of statistics.
// fsstat.go is related to mounted filesystems statistics
package stat

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"pgscv/app/log"
	"strings"
	"syscall"
)

// FsStat is the container for statistics of particular filesystem
type FsStat struct {
	Device            string // An underlying storage device
	Mountpoint        string // A directory where the filesystem is mounted
	Fstype            string // A type of the filesystem
	Mountflags        string // Flags which the filesystem mounted with
	TotalBytes        uint64 // Total size of the filesystem, in bytes
	UsedBytes         uint64 // Amount of used space, in bytes
	FreeBytes         uint64 // Amount of free space, including root-reserved space, in bytes
	AvailBytes        uint64 // Amount of space available for unprivileged users, in bytes
	RootReservedBytes uint64 // Amount of space reserved for root, in bytes
	RootReservedPct   uint64 // Amount of space reserved for root, in percents
	TotalInodes       uint64 // Total number of inodes in filesystem
	UsedInodes        uint64 // The number of used inodes
	FreeInodes        uint64 // The number of inodes available for use
}

// FsStats is an array for all filesystem statistics
type FsStats []FsStat

var (
	interestedFS = []string{"ext3", "ext4", "xfs", "brtfs"}
)

const (
	procMounts = "/proc/mounts"
)

// ReadLocal method read statistics about filesystem from 'procfs' filesystem
func (s *FsStats) ReadLocal() error {
	content, err := ioutil.ReadFile(procMounts)
	if err != nil {
		return nil
	}

	reader := bufio.NewReader(bytes.NewBuffer(content))
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}

		var fs FsStat
		var s1, s2 int
		_, err = fmt.Sscanf(string(line), "%s %s %s %s %d %d", &fs.Device, &fs.Mountpoint, &fs.Fstype, &fs.Mountflags, &s1, &s2)
		if err != nil {
			return nil
		}
		for _, fstype := range interestedFS {
			if fstype == fs.Fstype {
				var stat syscall.Statfs_t
				if err := syscall.Statfs(fs.Mountpoint, &stat); err != nil {
					return nil
				}

				fs.TotalBytes = stat.Blocks * uint64(stat.Bsize)
				fs.FreeBytes = stat.Bfree * uint64(stat.Bsize)
				fs.AvailBytes = stat.Bavail * uint64(stat.Bsize)
				fs.UsedBytes = (stat.Blocks - stat.Bfree) * uint64(stat.Bsize)
				fs.RootReservedBytes = (stat.Bfree - stat.Bavail) * uint64(stat.Bsize)
				fs.RootReservedPct = 100 * fs.RootReservedBytes / fs.TotalBytes
				fs.TotalInodes = stat.Files
				fs.FreeInodes = stat.Ffree
				fs.UsedInodes = stat.Files - stat.Ffree

				*s = append(*s, fs)
				break
			}
		}
	}

	return nil
}

// SingleStat method returns value of particular stat of filesystem
func (c FsStat) SingleStat(stat string) (value uint64) {
	switch stat {
	case "total_bytes":
		value = c.TotalBytes
	case "free_bytes":
		value = c.FreeBytes
	case "available_bytes":
		value = c.AvailBytes
	case "used_bytes":
		value = c.UsedBytes
	case "reserved_bytes":
		value = c.RootReservedBytes
	case "reserved_pct":
		value = c.RootReservedPct
	case "total_inodes":
		value = c.TotalInodes
	case "free_inodes":
		value = c.FreeInodes
	case "used_inodes":
		value = c.UsedInodes
	default:
		value = 0
	}
	return value
}

// ReadMounts returns list of pairs [mountpoint]device
func ReadMounts() map[string]string {
	var mountpoints = make(map[string]string)
	content, err := ioutil.ReadFile(procMounts)
	if err != nil {
		return nil
	}

	var device, mountpoint string
	reader := bufio.NewReader(bytes.NewBuffer(content))
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}

		if fields := strings.Fields(string(line)); len(fields) > 3 {
			device = fields[0]
			mountpoint = fields[1]
			for _, fstype := range interestedFS {
				if fstype == fields[2] {
					// dereference device-mapper
					if strings.HasPrefix(device, "/dev/mapper/") {
						device = resolveDeviceMapperName(device)
					}
					mountpoints[mountpoint] = device
				}
			}
		}
	}
	return mountpoints
}

// resolveDeviceMapperName translates symlinks from /dev/mapper/* to destination names, e.g. /dev/mapper/pgdb -> /dev/dm-4
func resolveDeviceMapperName(device string) string {
	device, err := os.Readlink(device)
	if err != nil {
		log.Warnf("failed to resolve symlink '%s' to origin: %s", device, err)
		return ""
	}
	return strings.Replace(device, "..", "/dev", 1)
}

// RewritePath searches symlinks, follows to origin and rewrite passed path
func RewritePath(path string) (string, error) {
	// TODO: might fail with more than one symlink used in the path =)
	parts := strings.Split(path, "/")
	for i := len(parts); i > 0; i-- {
		if subpath := strings.Join(parts[0:i], "/"); subpath != "" {
			// check is subpath a symlink, if symlink - dereference it
			fi, err := os.Lstat(subpath)
			if err != nil {
				return "", err
			}
			if fi.Mode()&os.ModeSymlink != 0 {
				resolvedLink, _ := os.Readlink(subpath)
				newpath := resolvedLink + "/" + strings.Join(parts[i:], "/")
				return newpath, nil
			}
		}
	}
	return path, nil
}
