package collector

import (
	"bufio"
	"fmt"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/procfs"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const (
	diskSectorSize        = 512
	ignoredDevicesPattern = "^(ram|loop|fd|(h|s|v|xv)d[a-z]|nvme\\d+n\\d+p)\\d+$"
)

type typedFactorDesc struct {
	desc      *prometheus.Desc
	valueType prometheus.ValueType
	factor    float64
}

func (d *typedFactorDesc) mustNewConstMetric(value float64, labels ...string) prometheus.Metric {
	if d.factor != 0 {
		value *= d.factor
	}
	return prometheus.MustNewConstMetric(d.desc, d.valueType, value, labels...)
}

type diskstatsCollector struct {
	ignoredDevicesPattern *regexp.Regexp
	descs                 []typedFactorDesc
}

// NewDiskstatsCollector returns a new Collector exposing disk device stats.
// Docs from https://www.kernel.org/doc/Documentation/iostats.txt
func NewDiskstatsCollector(labels prometheus.Labels) (Collector, error) {
	var diskLabelNames = []string{"device"}

	return &diskstatsCollector{
		ignoredDevicesPattern: regexp.MustCompile(ignoredDevicesPattern),
		descs: []typedFactorDesc{
			{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "disk", "reads_completed_total"),
					"The total number of reads completed successfully.",
					diskLabelNames, labels,
				), valueType: prometheus.CounterValue,
			},
			{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "disk", "reads_merged_total"),
					"The total number of reads merged.",
					diskLabelNames, labels,
				), valueType: prometheus.CounterValue,
			},
			{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "disk", "read_bytes_total"),
					"The total number of bytes read successfully.",
					diskLabelNames, labels,
				), valueType: prometheus.CounterValue, factor: diskSectorSize,
			},
			{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "disk", "read_time_seconds_total"),
					"The total number of seconds spent by all reads.",
					diskLabelNames, labels,
				), valueType: prometheus.CounterValue, factor: .001,
			},
			{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "disk", "writes_completed_total"),
					"The total number of writes completed successfully.",
					diskLabelNames, labels,
				), valueType: prometheus.CounterValue,
			},
			{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "disk", "writes_merged_total"),
					"The number of writes merged.",
					diskLabelNames, labels,
				), valueType: prometheus.CounterValue,
			},
			{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "disk", "written_bytes_total"),
					"The total number of bytes written successfully.",
					diskLabelNames, labels,
				), valueType: prometheus.CounterValue, factor: diskSectorSize,
			},
			{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "disk", "write_time_seconds_total"),
					"This is the total number of seconds spent by all writes.",
					diskLabelNames, labels,
				), valueType: prometheus.CounterValue, factor: .001,
			},
			{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "disk", "io_now"),
					"The number of I/Os currently in progress.",
					diskLabelNames, labels,
				), valueType: prometheus.GaugeValue,
			},
			{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "disk", "io_time_seconds_total"),
					"Total seconds spent doing I/Os.",
					diskLabelNames, labels,
				), valueType: prometheus.CounterValue, factor: .001,
			},
			{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "disk", "io_time_weighted_seconds_total"),
					"The weighted # of seconds spent doing I/Os.",
					diskLabelNames, labels,
				), valueType: prometheus.CounterValue, factor: .001,
			},
			{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "disk", "discards_completed_total"),
					"The total number of discards completed successfully.",
					diskLabelNames, labels,
				), valueType: prometheus.CounterValue,
			},
			{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "disk", "discards_merged_total"),
					"The total number of discards merged.",
					diskLabelNames, labels,
				), valueType: prometheus.CounterValue,
			},
			{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "disk", "discarded_sectors_total"),
					"The total number of sectors discarded successfully.",
					diskLabelNames, labels,
				), valueType: prometheus.CounterValue,
			},
			{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "disk", "discard_time_seconds_total"),
					"This is the total number of seconds spent by all discards.",
					diskLabelNames, labels,
				), valueType: prometheus.CounterValue, factor: .001,
			},
			{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "disk", "flush_requests_total"),
					"The total number of flush requests completed successfully",
					diskLabelNames, labels,
				), valueType: prometheus.CounterValue,
			},
			{
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "disk", "flush_requests_time_seconds_total"),
					"This is the total number of seconds spent by all flush requests.",
					diskLabelNames, labels,
				), valueType: prometheus.CounterValue, factor: .001,
			},
		},
	}, nil
}

func (c *diskstatsCollector) Update(ch chan<- prometheus.Metric) error {
	diskStats, err := getDiskStats()
	if err != nil {
		return fmt.Errorf("couldn't get diskstats: %w", err)
	}

	for dev, stats := range diskStats {
		if c.ignoredDevicesPattern.MatchString(dev) {
			log.Debugln("Ignoring device ", dev)
			continue
		}

		for i, value := range stats {
			// ignore unrecognized additional stats
			if i >= len(c.descs) {
				break
			}
			v, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return fmt.Errorf("invalid value %s in diskstats: %w", value, err)
			}
			ch <- c.descs[i].mustNewConstMetric(v, dev)
		}
	}
	return nil
}

func getDiskStats() (map[string][]string, error) {
	file, err := os.Open(filepath.Join(procfs.DefaultMountPoint, "diskstats"))
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	return parseDiskStats(file)
}

func parseDiskStats(r io.Reader) (map[string][]string, error) {
	var (
		diskStats = map[string][]string{}
		scanner   = bufio.NewScanner(r)
	)

	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())
		if len(parts) < 4 { // we strip major, minor and dev
			return nil, fmt.Errorf("invalid line in %s: %s", filepath.Join(procfs.DefaultMountPoint, "diskstats"), scanner.Text())
		}
		dev := parts[2]
		diskStats[dev] = parts[3:]
	}

	return diskStats, scanner.Err()
}
