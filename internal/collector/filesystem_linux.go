package collector

import (
	"bufio"
	"fmt"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"os"
	"regexp"
	"strings"
	"syscall"
)

const (
	reFilterFilesystemPattern = `^(ext3|ext4|xfs|btrfs)$`
)

type filesystemCollector struct {
	filterFilesystemPattern *regexp.Regexp
	bytes                   typedDesc
	files                   typedDesc
}

// NewFilesystemCollector returns a new Collector exposing filesystem stats.
func NewFilesystemCollector(labels prometheus.Labels) (Collector, error) {
	re, err := regexp.Compile(reFilterFilesystemPattern)
	if err != nil {
		return nil, err
	}

	return &filesystemCollector{
		filterFilesystemPattern: re,
		bytes: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "filesystem", "bytes"),
				"Total number of bytes of filesystem by each type.",
				[]string{"device", "mountpoint", "fstype", "type"}, labels,
			), valueType: prometheus.GaugeValue,
		},
		files: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "filesystem", "files"),
				"Total number of files (inodes) of filesystem by each type.",
				[]string{"device", "mountpoint", "fstype", "type"}, labels,
			), valueType: prometheus.GaugeValue,
		},
	}, nil
}

// Update method collects filesystem usage statistics.
func (c *filesystemCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
	stats, err := getFilesystemStats(c.filterFilesystemPattern)
	if err != nil {
		return fmt.Errorf("get filesystem stats failed: %s", err)
	}

	for _, s := range stats {
		// bytes
		ch <- c.bytes.mustNewConstMetric(s.size, s.device, s.mountpoint, s.fstype, "total")
		ch <- c.bytes.mustNewConstMetric(s.free, s.device, s.mountpoint, s.fstype, "free")
		ch <- c.bytes.mustNewConstMetric(s.avail, s.device, s.mountpoint, s.fstype, "avail")
		ch <- c.bytes.mustNewConstMetric(s.size-s.free, s.device, s.mountpoint, s.fstype, "used")
		// files (inodes)
		ch <- c.files.mustNewConstMetric(s.files, s.device, s.mountpoint, s.fstype, "total")
		ch <- c.files.mustNewConstMetric(s.filesfree, s.device, s.mountpoint, s.fstype, "free")
		ch <- c.files.mustNewConstMetric(s.files-s.filesfree, s.device, s.mountpoint, s.fstype, "used")
	}

	return nil
}

// filesystemStat describes various stats related to filesystem usage.
type filesystemStat struct {
	device     string
	mountpoint string
	fstype     string
	options    string
	size       float64
	free       float64
	avail      float64
	files      float64
	filesfree  float64
}

// getFilesystemStats opens stats file and execute stats parser.
func getFilesystemStats(filter *regexp.Regexp) ([]filesystemStat, error) {
	file, err := os.Open("/proc/mounts")
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	return parseFilesystemStats(file, filter)
}

// parseFilesystemStats parses stats file and return stats.
func parseFilesystemStats(r io.Reader, filter *regexp.Regexp) ([]filesystemStat, error) {
	stats, err := parseProcMounts(r, filter)
	if err != nil {
		return nil, err
	}

	for i, s := range stats {
		// TODO: add context with timeout
		var statFS syscall.Statfs_t
		if err := syscall.Statfs(s.mountpoint, &statFS); err != nil {
			log.Errorf("get stats for %s mountpoint failed: %s; skip", s.mountpoint, err)
			continue
		}

		stats[i] = filesystemStat{
			device:     s.device,
			mountpoint: s.mountpoint,
			fstype:     s.fstype,
			options:    s.options,
			size:       float64(statFS.Blocks) * float64(statFS.Bsize),
			free:       float64(statFS.Bfree) * float64(statFS.Bsize),
			avail:      float64(statFS.Bavail) * float64(statFS.Bsize),
			files:      float64(statFS.Files),
			filesfree:  float64(statFS.Ffree),
		}
	}

	return stats, nil
}

// parseProcMounts parses /proc/mounts and returns slice of stats with filled filesystems properties (but without stats values).
func parseProcMounts(r io.Reader, filter *regexp.Regexp) ([]filesystemStat, error) {
	var (
		scanner = bufio.NewScanner(r)
		stats   []filesystemStat
	)

	// Parse line by line, split line to param and value, parse the value to float and save to store.
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())

		if len(parts) != 6 {
			return nil, fmt.Errorf("/proc/mounts invalid line: %s; skip", scanner.Text())
		}

		fstype := parts[2]
		if filter != nil && !filter.MatchString(fstype) {
			log.Debugln("ignore filesystem ", fstype)
			continue
		}

		s := filesystemStat{
			device:     parts[0],
			mountpoint: parts[1],
			fstype:     fstype,
			options:    parts[3],
		}

		stats = append(stats, s)
	}

	return stats, scanner.Err()
}
