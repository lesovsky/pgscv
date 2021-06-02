package collector

import (
	"context"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/filter"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"io"
	"os"
	"sync"
	"syscall"
	"time"
)

type filesystemCollector struct {
	bytes      typedDesc
	bytesTotal typedDesc
	files      typedDesc
	filesTotal typedDesc
}

// NewFilesystemCollector returns a new Collector exposing filesystem stats.
func NewFilesystemCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {

	// Define default filters (if no already present) to avoid collecting metrics about exotic filesystems.
	if _, ok := settings.Filters["fstype"]; !ok {
		if settings.Filters == nil {
			settings.Filters = filter.New()
		}

		settings.Filters.Add("fstype", filter.Filter{Include: `^(ext3|ext4|xfs|btrfs)$`})
		err := settings.Filters.Compile()
		if err != nil {
			return nil, err
		}
	}

	return &filesystemCollector{
		bytes: newBuiltinTypedDesc(
			descOpts{"node", "filesystem", "bytes", "Number of bytes of filesystem by usage.", 0},
			prometheus.GaugeValue,
			[]string{"device", "mountpoint", "fstype", "usage"}, constLabels,
			settings.Filters,
		),
		bytesTotal: newBuiltinTypedDesc(
			descOpts{"node", "filesystem", "bytes_total", "Total number of bytes of filesystem capacity.", 0},
			prometheus.GaugeValue,
			[]string{"device", "mountpoint", "fstype"}, constLabels,
			settings.Filters,
		),
		files: newBuiltinTypedDesc(
			descOpts{"node", "filesystem", "files", "Number of files (inodes) of filesystem by usage.", 0},
			prometheus.GaugeValue,
			[]string{"device", "mountpoint", "fstype", "usage"}, constLabels,
			settings.Filters,
		),
		filesTotal: newBuiltinTypedDesc(
			descOpts{"node", "filesystem", "files_total", "Total number of files (inodes) of filesystem capacity.", 0},
			prometheus.GaugeValue,
			[]string{"device", "mountpoint", "fstype"}, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects filesystem usage statistics.
func (c *filesystemCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	stats, err := getFilesystemStats()
	if err != nil {
		return fmt.Errorf("get filesystem stats failed: %s", err)
	}

	for _, s := range stats {
		// Truncate device paths to device names, e.g /dev/sda -> sda
		device := truncateDeviceName(s.mount.device)

		// bytes; free = avail + reserved; total = used + free
		ch <- c.bytesTotal.newConstMetric(s.size, device, s.mount.mountpoint, s.mount.fstype)
		ch <- c.bytes.newConstMetric(s.avail, device, s.mount.mountpoint, s.mount.fstype, "avail")
		ch <- c.bytes.newConstMetric(s.free-s.avail, device, s.mount.mountpoint, s.mount.fstype, "reserved")
		ch <- c.bytes.newConstMetric(s.size-s.free, device, s.mount.mountpoint, s.mount.fstype, "used")
		// files (inodes)
		ch <- c.filesTotal.newConstMetric(s.files, device, s.mount.mountpoint, s.mount.fstype)
		ch <- c.files.newConstMetric(s.filesfree, device, s.mount.mountpoint, s.mount.fstype, "free")
		ch <- c.files.newConstMetric(s.files-s.filesfree, device, s.mount.mountpoint, s.mount.fstype, "used")
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
func getFilesystemStats() ([]filesystemStat, error) {
	file, err := os.Open("/proc/mounts")
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	return parseFilesystemStats(file)
}

// parseFilesystemStats parses stats file and return stats.
func parseFilesystemStats(r io.Reader) ([]filesystemStat, error) {
	mounts, err := parseProcMounts(r)
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}
	statCh := make(chan filesystemStat)
	stats := []filesystemStat{}

	wg.Add(len(mounts))
	for _, m := range mounts {
		mount := m

		// In pessimistic cases, filesystem might stuck and requesting stats might stuck too.
		// To avoid such situations wrap stats requests into context with timeout. One second
		// timeout should be sufficient for machines.
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

		// Requesting stats.
		go readMountpointStat(mount.mountpoint, statCh, &wg)

		// Awaiting the stats response from the channel, or context cancellation by timeout.
		select {
		case response := <-statCh:
			if response.err != nil {
				// Skip filesystems if getting its stats failed. This could occur quite often,
				// for example due to denied permissions.
				log.Debugf("get filesystem %s stats failed: %s; skip", mount.mountpoint, response.err)
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
