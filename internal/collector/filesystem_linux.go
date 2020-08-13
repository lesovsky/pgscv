package collector

import (
	"context"
	"fmt"
	"github.com/barcodepro/pgscv/internal/filter"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"os"
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
		device := truncateDeviceName(s.mount.device)

		// bytes
		ch <- c.bytes.mustNewConstMetric(s.size, device, s.mount.mountpoint, s.mount.fstype, "total")
		ch <- c.bytes.mustNewConstMetric(s.free, device, s.mount.mountpoint, s.mount.fstype, "free")
		ch <- c.bytes.mustNewConstMetric(s.avail, device, s.mount.mountpoint, s.mount.fstype, "avail")
		ch <- c.bytes.mustNewConstMetric(s.size-s.free, device, s.mount.mountpoint, s.mount.fstype, "used")
		// files (inodes)
		ch <- c.files.mustNewConstMetric(s.files, device, s.mount.mountpoint, s.mount.fstype, "total")
		ch <- c.files.mustNewConstMetric(s.filesfree, device, s.mount.mountpoint, s.mount.fstype, "free")
		ch <- c.files.mustNewConstMetric(s.files-s.filesfree, device, s.mount.mountpoint, s.mount.fstype, "used")
	}

	return nil
}

// filesystemStat describes various stats related to filesystem usage.
type filesystemStat struct {
	mount     mount
	size      float64
	free      float64
	avail     float64
	files     float64
	filesfree float64
	err       error // error occurred during polling stats
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
	mounts, err := parseProcMounts(r, filters)
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}
	statCh := make(chan filesystemStat)
	stats := []filesystemStat{}

	wg.Add(len(mounts))
	for _, m := range mounts {
		mount := m

		// In pessimistic cases, filesystem might stuck and requesting stats might stuck too. To avoid such situations wrap
		// stats requests into context with timeout. One second timeout should be sufficient for machines.
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

		// Requesting stats.
		go readMountpointStat(mount.mountpoint, statCh, &wg)

		// Awaiting the stats response from the channel, or context cancellation by timeout.
		select {
		case response := <-statCh:
			if response.err != nil {
				log.Errorf("get filesystem %s stats failed: %s; skip", mount.mountpoint, err)
				cancel()
				continue
			}

			stat := filesystemStat{
				mount:     mount,
				size:      response.size,
				free:      response.free,
				avail:     response.avail,
				files:     response.files,
				filesfree: response.filesfree,
			}
			stats = append(stats, stat)
		case <-ctx.Done():
			log.Warnf("filesystem %s doesn't respond: %s; skip", mount.mountpoint, ctx.Err())
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
		size:      float64(stat.Blocks) * float64(stat.Bsize),
		free:      float64(stat.Bfree) * float64(stat.Bsize),
		avail:     float64(stat.Bavail) * float64(stat.Bsize),
		files:     float64(stat.Files),
		filesfree: float64(stat.Ffree),
	}
}
