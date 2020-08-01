package collector

import (
	"bufio"
	"fmt"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
)

const (
	diskSectorSize        = 512
	ignoredDevicesPattern = "^(ram|loop|fd|(h|s|v|xv)d[a-z]|nvme\\d+n\\d+p)\\d+$"
)

type diskstatsCollector struct {
	ignoredDevicesPattern *regexp.Regexp
	completed             typedDesc
	merged                typedDesc
	bytes                 typedDesc
	times                 typedDesc
	ionow                 typedDesc
	iotime                typedDesc
	iotimeweighted        typedDesc
}

// NewDiskstatsCollector returns a new Collector exposing disk device stats.
// Docs from https://www.kernel.org/doc/Documentation/iostats.txt and https://www.kernel.org/doc/Documentation/ABI/testing/procfs-diskstats
func NewDiskstatsCollector(labels prometheus.Labels) (Collector, error) {
	var diskLabelNames = []string{"device", "type"}

	return &diskstatsCollector{
		ignoredDevicesPattern: regexp.MustCompile(ignoredDevicesPattern),
		completed: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "disk", "completed_total"),
				"The total number of IO requests completed successfully of each type.",
				diskLabelNames, labels,
			), valueType: prometheus.CounterValue,
		},
		merged: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "disk", "merged_total"),
				"The total number of merged IO requests of each type.",
				diskLabelNames, labels,
			), valueType: prometheus.CounterValue,
		},
		bytes: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "disk", "bytes_total"),
				"The total number of bytes processed by IO requests of each type.",
				diskLabelNames, labels,
			), valueType: prometheus.CounterValue, factor: diskSectorSize,
		},
		times: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "disk", "time_seconds_total"),
				"The total number of seconds spent on all requests of each type.",
				diskLabelNames, labels,
			), valueType: prometheus.CounterValue, factor: .001,
		},
		ionow: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "disk", "io_now"),
				"The number of I/Os currently in progress.",
				[]string{"device"}, labels,
			), valueType: prometheus.GaugeValue,
		},
		iotime: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "disk", "io_time_seconds_total"),
				"Total seconds spent doing I/Os.",
				[]string{"device"}, labels,
			), valueType: prometheus.CounterValue, factor: .001,
		},
		iotimeweighted: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "disk", "io_time_weighted_seconds_total"),
				"The weighted # of seconds spent doing I/Os.",
				[]string{"device"}, labels,
			), valueType: prometheus.CounterValue, factor: .001,
		},
	}, nil
}

func (c *diskstatsCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
	stats, err := getDiskstats(c.ignoredDevicesPattern)
	if err != nil {
		return fmt.Errorf("get diskstats failed: %s", err)
	}

	for dev, stat := range stats {
		if len(stat) >= 11 {
			ch <- c.completed.mustNewConstMetric(stat[0], dev, "reads")
			ch <- c.merged.mustNewConstMetric(stat[1], dev, "reads")
			ch <- c.bytes.mustNewConstMetric(stat[2], dev, "reads")
			ch <- c.times.mustNewConstMetric(stat[3], dev, "reads")
			ch <- c.completed.mustNewConstMetric(stat[4], dev, "writes")
			ch <- c.merged.mustNewConstMetric(stat[5], dev, "writes")
			ch <- c.bytes.mustNewConstMetric(stat[6], dev, "writes")
			ch <- c.times.mustNewConstMetric(stat[7], dev, "writes")
			ch <- c.ionow.mustNewConstMetric(stat[8], dev)
			ch <- c.iotime.mustNewConstMetric(stat[9], dev)
			ch <- c.iotimeweighted.mustNewConstMetric(stat[10], dev)
		}

		// for kernels 4.18+
		if len(stat) >= 15 {
			ch <- c.completed.mustNewConstMetric(stat[11], dev, "discards")
			ch <- c.merged.mustNewConstMetric(stat[12], dev, "discards")
			ch <- c.bytes.mustNewConstMetric(stat[13], dev, "discards")
			ch <- c.times.mustNewConstMetric(stat[14], dev, "discards")
		}

		// for kernels 5.5+
		if len(stat) >= 17 {
			ch <- c.completed.mustNewConstMetric(stat[15], dev, "flush")
			ch <- c.times.mustNewConstMetric(stat[16], dev, "flush")
		}
	}

	return nil
}

// getDiskstats opens stats file and executes stats parser.
func getDiskstats(ignore *regexp.Regexp) (map[string][]float64, error) {
	file, err := os.Open("/proc/diskstats")
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	return parseDiskstats(file, ignore)
}

// parseDiskstat reads stats file and returns stats structs.
func parseDiskstats(r io.Reader, ignore *regexp.Regexp) (map[string][]float64, error) {
	var scanner = bufio.NewScanner(r)
	var stats = map[string][]float64{}

	for scanner.Scan() {
		values := strings.Fields(scanner.Text())

		// Linux kernel <= 4.18 have 14 columns, 4.18+ have 18, 5.5+ have 20 columns
		// for details see https://www.kernel.org/doc/Documentation/ABI/testing/procfs-diskstats)
		if len(values) != 14 && len(values) != 18 && len(values) != 20 {
			return nil, fmt.Errorf("invalid /proc/diskstats file, too few columns in line: %s", scanner.Text())
		}

		var device = values[2]
		if ignore != nil && ignore.MatchString(device) {
			log.Debugln("ignore device ", device)
			continue
		}

		// Create float64 slice for values, parse line except first three values (major/minor/device)
		stat := make([]float64, len(values)-3)
		for i := range stat {
			value, err := strconv.ParseFloat(values[i+3], 64)
			if err != nil {
				log.Errorf("convert string to float64 failed: %s; skip", err)
				continue
			}
			stat[i] = value
		}

		stats[device] = stat
	}

	return stats, scanner.Err()
}
