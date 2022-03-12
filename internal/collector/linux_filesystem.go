package collector

import (
	"errors"
	"fmt"
	"github.com/lesovsky/pgscv/internal/filter"
	"github.com/lesovsky/pgscv/internal/log"
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"os"
	"syscall"
	"time"
)

var (
	errFilesystemTimedOut = errors.New("filesystem timed out")
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
func (c *filesystemCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
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

	var stats []filesystemStat
	for _, m := range mounts {
		stat, err := readMountpointStat(m.mountpoint)
		if err != nil {
			log.Warnf("read %s stats failed: %s", m.mountpoint, err)
			continue
		}

		stats = append(stats, filesystemStat{
			mount:     m,
			size:      stat.size,
			free:      stat.free,
			avail:     stat.avail,
			files:     stat.files,
			filesfree: stat.filesfree,
		})
	}

	return stats, nil
}

// readMountpointStat requests stats from kernel and return filesystemStat if successful.
func readMountpointStat(mountpoint string) (filesystemStat, error) {
	// Reading filesystem statistics might stuck, especially this is true for network filesystems.
	// In such case reading stats done by child goroutine with timeout and allow it to hang. When
	// timeout exceeds outside of child, return an error and left behind the spawned goroutine (it
	// is impossible to forcibly interrupt stuck syscall). Hope when syscall finished at all, stat
	// is discarded and goroutine finishes normally.

	timeout := 3 * time.Second // three seconds is sufficient to consider filesystem unresponsive.
	statCh := make(chan *syscall.Statfs_t)
	errCh := make(chan error)

	// Run goroutine with reading stats. Check kind of returned error. If error related to timeout,
	// print warning and return. Other kinds of error should be reported to parent.
	go func() {
		s, err := readMountpointStatWithTimeout(mountpoint, timeout)
		if err != nil {
			if err == errFilesystemTimedOut {
				log.Warnf("%s: %s, skip", mountpoint, err)
				return
			}
			errCh <- err
		}

		// Syscall successful - send stat to the channel.
		statCh <- s
	}()

	// Waiting for results of spawned goroutine or time out.
	for {
		select {
		case s := <-statCh:
			return filesystemStat{
				size:      float64(s.Blocks) * float64(s.Bsize),
				free:      float64(s.Bfree) * float64(s.Bsize),
				avail:     float64(s.Bavail) * float64(s.Bsize),
				files:     float64(s.Files),
				filesfree: float64(s.Ffree),
			}, nil
		case err := <-errCh:
			return filesystemStat{err: err}, err
		case <-time.After(timeout):
			// Timeout expired, filesystem considered stuck, return.
			return filesystemStat{err: errFilesystemTimedOut}, errFilesystemTimedOut
		}
	}
}

// readMountpointStatWithTimeout read filesystem stats, discard data if reading exceeds timeout.
func readMountpointStatWithTimeout(mountpoint string, timeout time.Duration) (*syscall.Statfs_t, error) {
	var buf syscall.Statfs_t
	start := time.Now()

	err := syscall.Statfs(mountpoint, &buf)
	if err != nil {
		return nil, err
	}

	if time.Since(start) > timeout {
		log.Warnf("%s stats stale: %s", mountpoint, time.Since(start).String())
		return nil, errFilesystemTimedOut
	}

	return &buf, nil
}
