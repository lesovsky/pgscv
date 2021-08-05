package collector

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/filter"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const (
	// Linux always considers sectors to be 512 bytes long independently of the devices real block size.
	// https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/include/linux/types.h#n117
	diskSectorSize = 512
)

type diskstatsCollector struct {
	completed      typedDesc
	completedAll   typedDesc
	merged         typedDesc
	mergedAll      typedDesc
	bytes          typedDesc
	bytesAll       typedDesc
	times          typedDesc
	timesAll       typedDesc
	ionow          typedDesc
	iotime         typedDesc
	iotimeweighted typedDesc
	storageInfo    typedDesc
	storageSize    typedDesc
}

// NewDiskstatsCollector returns a new Collector exposing disk device stats.
// Docs from https://www.kernel.org/doc/Documentation/iostats.txt and https://www.kernel.org/doc/Documentation/ABI/testing/procfs-diskstats
func NewDiskstatsCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {

	// Define default filters (if no already present) to avoid collecting metrics about virtual devices and device partitions.
	if _, ok := settings.Filters["device"]; !ok {
		if settings.Filters == nil {
			settings.Filters = filter.New()
		}

		settings.Filters.Add("device", filter.Filter{Exclude: `^(ram|loop|fd|sr|(h|s|v|xv)d[a-z]|nvme\d+n\d+p)\d+$`})
		err := settings.Filters.Compile()
		if err != nil {
			return nil, err
		}
	}

	diskLabelNames := []string{"device", "type"}

	return &diskstatsCollector{
		completed: newBuiltinTypedDesc(
			descOpts{"node", "disk", "completed_total", "The total number of IO requests completed successfully of each type.", 0},
			prometheus.CounterValue,
			diskLabelNames, constLabels,
			settings.Filters,
		),
		completedAll: newBuiltinTypedDesc(
			descOpts{"node", "disk", "completed_all_total", "The total number of IO requests completed successfully.", 0},
			prometheus.CounterValue,
			[]string{"device"}, constLabels,
			settings.Filters,
		),
		merged: newBuiltinTypedDesc(
			descOpts{"node", "disk", "merged_total", "The total number of merged IO requests of each type.", 0},
			prometheus.CounterValue,
			diskLabelNames, constLabels,
			settings.Filters,
		),
		mergedAll: newBuiltinTypedDesc(
			descOpts{"node", "disk", "merged_all_total", "The total number of merged IO requests.", 0},
			prometheus.CounterValue,
			[]string{"device"}, constLabels,
			settings.Filters,
		),
		bytes: newBuiltinTypedDesc(
			descOpts{"node", "disk", "bytes_total", "The total number of bytes processed by IO requests of each type.", diskSectorSize},
			prometheus.CounterValue,
			diskLabelNames, constLabels,
			settings.Filters,
		),
		bytesAll: newBuiltinTypedDesc(
			descOpts{"node", "disk", "bytes_all_total", "The total number of bytes processed by IO requests.", diskSectorSize},
			prometheus.CounterValue,
			[]string{"device"}, constLabels,
			settings.Filters,
		),
		times: newBuiltinTypedDesc(
			descOpts{"node", "disk", "time_seconds_total", "The total number of seconds spent on all requests of each type.", .001},
			prometheus.CounterValue,
			diskLabelNames, constLabels,
			settings.Filters,
		),
		timesAll: newBuiltinTypedDesc(
			descOpts{"node", "disk", "time_seconds_all_total", "The total number of seconds spent on all requests.", .001},
			prometheus.CounterValue,
			[]string{"device"}, constLabels,
			settings.Filters,
		),
		ionow: newBuiltinTypedDesc(
			descOpts{"node", "disk", "io_now", "The number of I/Os currently in progress.", 0},
			prometheus.GaugeValue,
			[]string{"device"}, constLabels,
			settings.Filters,
		),
		iotime: newBuiltinTypedDesc(
			descOpts{"node", "disk", "io_time_seconds_total", "Total seconds spent doing I/Os.", .001},
			prometheus.CounterValue,
			[]string{"device"}, constLabels,
			settings.Filters,
		),
		iotimeweighted: newBuiltinTypedDesc(
			descOpts{"node", "disk", "io_time_weighted_seconds_total", "The weighted number of seconds spent doing I/Os.", .001},
			prometheus.CounterValue,
			[]string{"device"}, constLabels,
			settings.Filters,
		),
		// DEPRECATED.
		storageInfo: newBuiltinTypedDesc(
			descOpts{"node", "system", "storage_info", "Labeled information about storage devices present in the system. DEPRECATED: consider using node_system_storage_size_bytes.", 0},
			prometheus.GaugeValue,
			[]string{"device", "rotational", "scheduler"}, constLabels,
			settings.Filters,
		),
		storageSize: newBuiltinTypedDesc(
			descOpts{"node", "system", "storage_size_bytes", "Total size of storage device in bytes.", diskSectorSize},
			prometheus.GaugeValue,
			[]string{"device", "rotational", "scheduler"}, constLabels,
			settings.Filters,
		),
	}, nil
}

func (c *diskstatsCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	stats, err := getDiskstats()
	if err != nil {
		return fmt.Errorf("get diskstats failed: %s", err)
	}

	for dev, stat := range stats {
		// totals
		var completedTotal, mergedTotal, bytesTotal, secondsTotal float64

		if len(stat) >= 11 {
			completedTotal = stat[0] + stat[4]
			mergedTotal = stat[1] + stat[5]
			bytesTotal = stat[2] + stat[6]
			secondsTotal = stat[3] + stat[7]
			ch <- c.completed.newConstMetric(stat[0], dev, "read")
			ch <- c.merged.newConstMetric(stat[1], dev, "read")
			ch <- c.bytes.newConstMetric(stat[2], dev, "read")
			ch <- c.times.newConstMetric(stat[3], dev, "read")
			ch <- c.completed.newConstMetric(stat[4], dev, "write")
			ch <- c.merged.newConstMetric(stat[5], dev, "write")
			ch <- c.bytes.newConstMetric(stat[6], dev, "write")
			ch <- c.times.newConstMetric(stat[7], dev, "write")
			ch <- c.ionow.newConstMetric(stat[8], dev)
			ch <- c.iotime.newConstMetric(stat[9], dev)
			ch <- c.iotimeweighted.newConstMetric(stat[10], dev)
		}

		// for kernels 4.18+
		if len(stat) >= 15 {
			completedTotal += stat[11]
			mergedTotal += stat[12]
			bytesTotal += stat[13]
			secondsTotal += stat[14]
			ch <- c.completed.newConstMetric(stat[11], dev, "discard")
			ch <- c.merged.newConstMetric(stat[12], dev, "discard")
			ch <- c.bytes.newConstMetric(stat[13], dev, "discard")
			ch <- c.times.newConstMetric(stat[14], dev, "discard")
		}

		// for kernels 5.5+
		if len(stat) >= 17 {
			completedTotal += stat[15]
			secondsTotal += stat[16]
			ch <- c.completed.newConstMetric(stat[15], dev, "flush")
			ch <- c.times.newConstMetric(stat[16], dev, "flush")
		}

		// Send accumulated totals.
		ch <- c.completedAll.newConstMetric(completedTotal, dev)
		ch <- c.mergedAll.newConstMetric(mergedTotal, dev)
		ch <- c.bytesAll.newConstMetric(bytesTotal, dev)
		ch <- c.timesAll.newConstMetric(secondsTotal, dev)
	}

	// Collect storages properties.
	storages, err := getStorageProperties("/sys/block/*")
	if err != nil {
		log.Warnf("get storage devices properties failed: %s; skip", err)
	} else {
		for _, s := range storages {
			ch <- c.storageInfo.newConstMetric(1, s.device, s.rotational, s.scheduler)
			ch <- c.storageSize.newConstMetric(float64(s.size), s.device, s.rotational, s.scheduler)
		}
	}

	return nil
}

// getDiskstats opens stats file and executes stats parser.
func getDiskstats() (map[string][]float64, error) {
	file, err := os.Open("/proc/diskstats")
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	return parseDiskstats(file)
}

// parseDiskstat reads stats file and returns stats structs.
func parseDiskstats(r io.Reader) (map[string][]float64, error) {
	log.Debug("parse disk stats")

	var scanner = bufio.NewScanner(r)
	var stats = map[string][]float64{}

	for scanner.Scan() {
		values := strings.Fields(scanner.Text())

		// Linux kernel <= 4.18 have 14 columns, 4.18+ have 18, 5.5+ have 20 columns
		// for details see https://www.kernel.org/doc/Documentation/ABI/testing/procfs-diskstats)
		if len(values) != 14 && len(values) != 18 && len(values) != 20 {
			return nil, fmt.Errorf("invalid input, '%s': wrong number of values", scanner.Text())
		}

		device := values[2]

		// Create float64 slice for values, parse line except first three values (major/minor/device)
		stat := make([]float64, len(values)-3)
		for i := range stat {
			value, err := strconv.ParseFloat(values[i+3], 64)
			if err != nil {
				log.Errorf("invalid input, parse '%s' failed: %s, skip", values[i+3], err.Error())
				continue
			}
			stat[i] = value
		}

		stats[device] = stat
	}

	return stats, scanner.Err()
}

// storageDeviceProperties defines storage devices properties observed through /sys/block/* interface.
type storageDeviceProperties struct {
	device     string
	rotational string
	scheduler  string
	size       int64
}

// getStorageProperties reads storages properties.
func getStorageProperties(path string) ([]storageDeviceProperties, error) {
	log.Debugf("parse storage properties: %s", path)

	dirs, err := filepath.Glob(path)
	if err != nil {
		return nil, err
	}

	var storages = []storageDeviceProperties{}

	for _, devpath := range dirs {
		parts := strings.Split(devpath, "/")
		device := parts[len(parts)-1]

		// Read 'rotational' property.
		rotational, err := getDeviceRotational(devpath)
		if err != nil {
			log.Warnf("get 'rotational' for %s failed: %s; skip", device, err)
			continue
		}

		// Read 'scheduler' property.
		scheduler, err := getDeviceScheduler(devpath)
		if err != nil {
			log.Warnf("get 'scheduler' for %s failed: %s; skip", device, err)
			continue
		}

		size, err := getDeviceSize(devpath)
		if err != nil {
			log.Warnf("get size for %s failed: %s; skip", device, err)
			continue
		}

		storages = append(storages, storageDeviceProperties{
			device:     device,
			scheduler:  scheduler,
			rotational: rotational,
			size:       size,
		})
	}
	return storages, nil
}

// getDeviceRotational returns device's 'rotational' property.
func getDeviceRotational(devpath string) (string, error) {
	rotationalFile := devpath + "/queue/rotational"

	content, err := os.ReadFile(filepath.Clean(rotationalFile))
	if err != nil {
		return "", err
	}
	reader := bufio.NewReader(bytes.NewBuffer(content))
	line, _, err := reader.ReadLine()
	if err != nil {
		return "", err
	}

	switch string(line) {
	case "0", "1":
		return string(line), nil
	default:
		return "", fmt.Errorf("unknown rotational %s", string(line))
	}
}

// getDeviceScheduler returns name of the IO scheduler used by device.
func getDeviceScheduler(devpath string) (string, error) {
	re, err := regexp.Compile(`[[a-z-]+]|none`)
	if err != nil {
		return "", err
	}

	schedulerFile := devpath + "/queue/scheduler"

	content, err := os.ReadFile(filepath.Clean(schedulerFile))
	if err != nil {
		return "", err
	}
	reader := bufio.NewReader(bytes.NewBuffer(content))
	line, _, err := reader.ReadLine()
	if err != nil {
		return "", err
	}

	if sched := re.Find(line); sched != nil {
		return string(bytes.Trim(sched, "[]")), nil
	}

	return "", fmt.Errorf("unknown scheduler: %s", line)
}

// getDeviceSize returns size of the device in sectors.
func getDeviceSize(devpath string) (int64, error) {
	sizeStr, err := os.ReadFile(devpath + "/size")
	if err != nil {
		return 0, err
	}

	size, err := strconv.ParseInt(strings.TrimSpace(string(sizeStr)), 10, 64)
	if err != nil {
		return 0, err
	}

	return size, nil
}
