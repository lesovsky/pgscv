package stat

import (
	"bufio"
	"bytes"
	"io"
	"fmt"
	"syscall"
	"io/ioutil"
)

type FsStat struct {
	Device          string			// An underlying storage device
	Mountpoint      string			// A directory where the filesystem is mounted
	Fstype          string			// A type of the filesystem
	Mountflags      string			// Flags which the filesystem mounted with
	TotalBytes      uint64			// Total size of the filesystem, in bytes
	UsedBytes       uint64			// Amount of used space, in bytes
	FreeBytes       uint64			// Amount of free space, including root-reserved space, in bytes
	AvailBytes      uint64			// Amount of space available for unprivileged users, in bytes
	RootReservedBytes       uint64	// Amount of space reserved for root, in bytes
	RootReservedPct         uint64	// Amount of space reserved for root, in percents
	TotalInodes     uint64			// Total number of inodes in filesystem
	UsedInodes      uint64			// The number of used inodes
	FreeInodes      uint64    		// The number of inodes available for use
}

type FsStats []FsStat

var (
	interestedFS = []string{"ext3", "ext4", "xfs", "brtfs"}
)

const (
	PROC_MOUNTS = "/proc/mounts"
)

func (s *FsStats) ReadLocal() error {
	content, err := ioutil.ReadFile(PROC_MOUNTS)
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

// Function returns value of particular stat of an interface
func (c FsStat) SingleStat(stat string) (value uint64) {
	switch stat {
	case "total_bytes": value = c.TotalBytes
	case "free_bytes": value = c.FreeBytes
	case "available_bytes": value = c.AvailBytes
	case "used_bytes": value = c.UsedBytes
	case "reserved_bytes": value = c.RootReservedBytes
	case "reserved_pct": value = c.RootReservedPct
	case "total_inodes": value = c.TotalInodes
	case "free_inodes": value = c.FreeInodes
	case "used_inodes": value = c.UsedInodes
	default: value = 0
	}
	return value
}
