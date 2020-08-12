package collector

import (
	"bufio"
	"context"
	"fmt"
	"github.com/barcodepro/pgscv/internal/filter"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
)

type filesystemCollector struct {
	bytes typedDesc
	files typedDesc
}

// NewFilesystemCollector returns a new Collector exposing filesystem stats.
func NewFilesystemCollector(labels prometheus.Labels) (Collector, error) {
	return &filesystemCollector{
		bytes: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "filesystem", "bytes"),
				"Total number of bytes of filesystem by each type.",
				[]string{"device", "mountpoint", "fstype", "usage"}, labels,
			), valueType: prometheus.GaugeValue,
		},
		files: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "filesystem", "files"),
				"Total number of files (inodes) of filesystem by each type.",
				[]string{"device", "mountpoint", "fstype", "usage"}, labels,
			), valueType: prometheus.GaugeValue,
		},
	}, nil
}

// Update method collects filesystem usage statistics.
func (c *filesystemCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	stats, err := getFilesystemStats(config.Filters)
	if err != nil {
		return fmt.Errorf("get filesystem stats failed: %s", err)
	}

	for _, s := range stats {
		// Truncate device paths to device names, e.g /dev/sda -> sda
		device, err := truncateDeviceName(s.device)
		if err != nil {
			log.Warnf("truncate device path %s failed: %s; skip", device, err)
			continue
		}

		// bytes
		ch <- c.bytes.mustNewConstMetric(s.size, device, s.mountpoint, s.fstype, "total")
		ch <- c.bytes.mustNewConstMetric(s.free, device, s.mountpoint, s.fstype, "free")
		ch <- c.bytes.mustNewConstMetric(s.avail, device, s.mountpoint, s.fstype, "avail")
		ch <- c.bytes.mustNewConstMetric(s.size-s.free, device, s.mountpoint, s.fstype, "used")
		// files (inodes)
		ch <- c.files.mustNewConstMetric(s.files, device, s.mountpoint, s.fstype, "total")
		ch <- c.files.mustNewConstMetric(s.filesfree, device, s.mountpoint, s.fstype, "free")
		ch <- c.files.mustNewConstMetric(s.files-s.filesfree, device, s.mountpoint, s.fstype, "used")
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
func getFilesystemStats(filters map[string]filter.Filter) ([]filesystemStat, error) {
	file, err := os.Open("/proc/mounts")
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	return parseFilesystemStats(file, filters)
}

// parseFilesystemStats parses stats file and return stats.
func parseFilesystemStats(r io.Reader, filters map[string]filter.Filter) ([]filesystemStat, error) {
	stats, err := parseProcMounts(r, filters)
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}
	statCh := make(chan filesystemStat)

	wg.Add(len(stats))
	for i, s := range stats {
		stat := s

		// In pessimistic cases, filesystem might stuck and requesting stats might stuck too. To avoid such situations wrap
		// stats requests into context with timeout. One second timeout should be sufficient for machines.
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

		// Requesting stats.
		go readMountpointStat(stat.mountpoint, statCh, &wg)

		// Awaiting the stats response from the channel, or context cancellation by timeout.
		select {
		case response := <-statCh:
			if response.err != nil {
				log.Errorf("get filesystem %s stats failed: %s; skip", stat.mountpoint, err)
				cancel()
				continue
			}

			stats[i] = filesystemStat{
				device:     stat.device,
				mountpoint: stat.mountpoint,
				fstype:     stat.fstype,
				options:    stat.options,
				size:       response.size,
				free:       response.free,
				avail:      response.avail,
				files:      response.files,
				filesfree:  response.filesfree,
			}
		case <-ctx.Done():
			log.Warnf("filesystem %s doesn't respond: %s; skip", s.mountpoint, ctx.Err())
			cancel()
			continue
		}

		cancel()
	}

	wg.Wait()
	close(statCh)
	return stats, nil
}

// readMountpointStat requests stats from kernel and sends stats to channel.
func readMountpointStat(mountpoint string, ch chan filesystemStat, wg *sync.WaitGroup) {
	defer wg.Done()

	var stat syscall.Statfs_t
	if err := syscall.Statfs(mountpoint, &stat); err != nil {
		ch <- filesystemStat{err: err}
		return
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
func parseProcMounts(r io.Reader, filters map[string]filter.Filter) ([]filesystemStat, error) {
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
		if f, ok := filters["filesystem/fstype"]; ok {
			if !f.Pass(fstype) {
				log.Debugln("ignore filesystem ", fstype)
				continue
			}
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
