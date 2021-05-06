package collector

import (
	"bufio"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type meminfoCollector struct {
	re          *regexp.Regexp
	constLabels prometheus.Labels
	memused     typedDesc
	swapused    typedDesc
}

// NewMeminfoCollector returns a new Collector exposing memory stats.
func NewMeminfoCollector(labels prometheus.Labels, settings model.CollectorSettings) (Collector, error) {
	return &meminfoCollector{
		re:          regexp.MustCompile(`\((.*)\)`),
		constLabels: labels,
		memused: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "memory", "MemUsed"),
				"Memory information composite field MemUsed.",
				nil, labels,
			), valueType: prometheus.GaugeValue,
		},
		swapused: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "memory", "SwapUsed"),
				"Memory information composite field SwapUsed.",
				nil, labels,
			), valueType: prometheus.GaugeValue,
		},
	}, nil
}

// Update method collects network interfaces statistics.
func (c *meminfoCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
	meminfo, err := getMeminfoStats()
	if err != nil {
		return fmt.Errorf("get /proc/meminfo stats failed: %s", err)
	}

	vmstat, err := getVmstatStats()
	if err != nil {
		return fmt.Errorf("get /proc/vmstat stats failed: %s", err)
	}

	// Processing meminfo stats.
	for param, value := range meminfo {
		param = c.re.ReplaceAllString(param, "_${1}")
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName("node", "memory", param),
				fmt.Sprintf("Memory information field %s.", param),
				nil, c.constLabels,
			), prometheus.GaugeValue, value,
		)
	}

	// MemUsed and SwapUsed are composite metrics and not present in /proc/meminfo.
	ch <- c.memused.mustNewConstMetric(meminfo["MemTotal"] - meminfo["MemFree"] - meminfo["Buffers"] - meminfo["Cached"])
	ch <- c.swapused.mustNewConstMetric(meminfo["SwapTotal"] - meminfo["SwapFree"])

	// Processing vmstat stats.
	for param, value := range vmstat {
		// Depending on key name, make an assumption about metric type.
		// Analyzing of vmstat content shows that gauge values have 'nr_' prefix. But without of
		// strong knowledge of kernel internals this is just an assumption and could be mistaken.
		t := prometheus.CounterValue
		if strings.HasPrefix(param, "nr_") {
			t = prometheus.GaugeValue
		}

		param = c.re.ReplaceAllString(param, "_${1}")

		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName("node", "vmstat", param),
				fmt.Sprintf("Vmstat information field %s.", param),
				nil, c.constLabels,
			), t, value,
		)
	}

	return nil
}

// getMeminfoStats is the intermediate function which opens stats file and run stats parser for extracting stats.
func getMeminfoStats() (map[string]float64, error) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	return parseMeminfoStats(file)
}

// parseMeminfoStats accepts file descriptor, reads file content and produces stats.
func parseMeminfoStats(r io.Reader) (map[string]float64, error) {
	log.Debug("parse meminfo stats")

	var (
		scanner = bufio.NewScanner(r)
		stats   = map[string]float64{}
	)

	// Parse line by line, split line to param and value, parse the value to float and save to store.
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())

		if len(parts) < 2 || len(parts) > 3 {
			return nil, fmt.Errorf("invalid input, '%s': wrong number of values", scanner.Text())
		}

		param, value := strings.TrimRight(parts[0], ":"), parts[1]

		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			log.Errorf("invalid input, parse '%s' failed: %s, skip", value, err.Error())
			continue
		}

		if len(parts) == 3 && parts[2] == "kB" {
			v *= 1024
		}

		stats[param] = v
	}

	return stats, scanner.Err()
}

// getVmstatStats is the intermediate function which opens stats file and run stats parser for extracting stats.
func getVmstatStats() (map[string]float64, error) {
	file, err := os.Open("/proc/vmstat")
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	return parseVmstatStats(file)
}

// parseVmstatStats accepts file descriptor, reads file content and produces stats.
func parseVmstatStats(r io.Reader) (map[string]float64, error) {
	log.Debug("parse vmstat stats")

	var (
		scanner = bufio.NewScanner(r)
		stats   = map[string]float64{}
	)

	// Parse line by line, split line to param and value, parse the value to float and save to store.
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())

		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid input, '%s': wrong number of values", scanner.Text())
		}

		param, value := parts[0], parts[1]

		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			log.Errorf("invalid input, parse '%s' failed: %s, skip", value, err.Error())
			continue
		}

		stats[param] = v
	}

	return stats, scanner.Err()
}
