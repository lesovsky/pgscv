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

type meminfoCollector struct {
	re          *regexp.Regexp
	meminfo     typedDesc
	constLabels prometheus.Labels
}

// NewMeminfoCollector returns a new Collector exposing memory stats.
func NewMeminfoCollector(labels prometheus.Labels) (Collector, error) {
	return &meminfoCollector{
		re:          regexp.MustCompile(`\((.*)\)`),
		constLabels: labels,
		meminfo: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "memory", "meminfo"),
				"Memory information based on /proc/meminfo.",
				[]string{"usage"}, labels,
			),
			valueType: prometheus.GaugeValue,
		},
	}, nil
}

// Update method collects network interfaces statistics
func (c *meminfoCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
	stats, err := getMeminfoStats()
	if err != nil {
		return fmt.Errorf("get /proc/meminfo stats failed: %s", err)
	}

	for param, value := range stats {
		param = c.re.ReplaceAllString(param, "_${1}")
		ch <- c.meminfo.mustNewConstMetric(value, param)
	}

	// MemUsed and SwapUsed are composite metrics and not present in /proc/meminfo.
	memused := stats["MemTotal"] - stats["MemFree"] - stats["Buffers"] - stats["Cached"]
	swapused := stats["SwapTotal"] - stats["SwapFree"]
	ch <- c.meminfo.mustNewConstMetric(memused, "MemUsed")
	ch <- c.meminfo.mustNewConstMetric(swapused, "SwapUsed")

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
	var (
		scanner = bufio.NewScanner(r)
		stats   = map[string]float64{}
	)

	// Parse line by line, split line to param and value, parse the value to float and save to store.
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())

		if len(parts) < 2 || len(parts) > 3 {
			return nil, fmt.Errorf("/proc/meminfo invalid line: %s; skip", scanner.Text())
		}

		var param = strings.TrimRight(parts[0], ":")
		var value = parts[1]

		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			log.Errorf("convert string to float64 failed: %s; skip", err)
			continue
		}

		if len(parts) == 3 && parts[2] == "kB" {
			v *= 1024
		}

		stats[param] = v
	}

	return stats, scanner.Err()
}
