package collector

import (
	"bufio"
	"context"
	"fmt"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"os"
	"regexp"
	"strings"
	"syscall"
	"time"
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
	err        error // error occurred during polling stats
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

	statCh := make(chan filesystemStat)
	defer close(statCh)

	for i, s := range stats {
		// In pessimistic cases, filesystem might stuck and requesting stats might stuck too. To avoid such situations wrap
		// stats requests into context with timeout. One second timeout should be sufficient for machines.
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

		// Requesting stats.
		go readMountpointStat(s.mountpoint, statCh)

		// Awaiting the stats response from the channel, or context cancellation by timeout.
		select {
		case response := <-statCh:
			if response.err != nil {
				log.Errorf("get filesystem %s stats failed: %s; skip", s.mountpoint, err)
				cancel()
				continue
			}

			stats[i] = filesystemStat{
				device:     s.device,
				mountpoint: s.mountpoint,
				fstype:     s.fstype,
				options:    s.options,
				size:       response.size,
				free:       response.free,
				avail:      response.avail,
				files:      response.files,
				filesfree:  response.filesfree,
			}
		case <-ctx.Done():
			log.Warnf("filesystem %s doesn't respond: %s; skip", s.mountpoint, ctx.Err())
			continue
		}

		cancel()
	}

	return stats, nil
}

// readMountpointStat requests stats from kernel and sends stats to channel.
func readMountpointStat(mountpoint string, ch chan filesystemStat) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(mountpoint, &stat); err != nil {
		ch <- filesystemStat{err: err}
	}

	// Syscall successful - send stat to the channel.
	ch <- filesystemStat{
		mountpoint: mountpoint,
		size:       float64(stat.Blocks) * float64(stat.Bsize),
		free:       float64(stat.Bfree) * float64(stat.Bsize),
		avail:      float64(stat.Bavail) * float64(stat.Bsize),
		files:      float64(stat.Files),
		filesfree:  float64(stat.Ffree),
	}
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
