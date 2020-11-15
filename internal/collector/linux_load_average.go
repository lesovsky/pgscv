package collector

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"io/ioutil"
	"strconv"
	"strings"
)

type loadaverageCollector struct {
	load1  typedDesc
	load5  typedDesc
	load15 typedDesc
}

// NewLoadAverageCollector returns a new Collector exposing load average statistics.
func NewLoadAverageCollector(labels prometheus.Labels) (Collector, error) {
	return &loadaverageCollector{
		load1: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "", "load1"),
				"1m load average.",
				nil, labels,
			), valueType: prometheus.GaugeValue,
		},
		load5: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "", "load5"),
				"5m load average.",
				nil, labels,
			), valueType: prometheus.GaugeValue,
		},
		load15: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "", "load15"),
				"15m load average.",
				nil, labels,
			), valueType: prometheus.GaugeValue,
		},
	}, nil
}

// Update implements Collector and exposes load average related metrics from /proc/loadavg.
func (c *loadaverageCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
	stats, err := getLoadAverageStats()
	if err != nil {
		return fmt.Errorf("get load average stats failed: %s", err)
	}

	ch <- c.load1.mustNewConstMetric(stats[0])
	ch <- c.load5.mustNewConstMetric(stats[1])
	ch <- c.load15.mustNewConstMetric(stats[2])

	return nil
}

// getLoadAverageStats reads /proc/loadavg and return load stats.
func getLoadAverageStats() ([]float64, error) {
	data, err := ioutil.ReadFile("/proc/loadavg")
	if err != nil {
		return nil, err
	}

	return parseLoadAverageStats(string(data))
}

// parseLoadAverageStats parses content from /proc/loadavg and return load stats.
func parseLoadAverageStats(data string) ([]float64, error) {
	parts := strings.Fields(data)
	if len(parts) < 3 {
		return nil, fmt.Errorf("/proc/loadavg bad content: %s", data)
	}

	var err error
	loads := make([]float64, 3)
	for i, load := range parts[0:3] {
		loads[i], err = strconv.ParseFloat(load, 64)
		if err != nil {
			return nil, fmt.Errorf("parse /proc/loadavg value '%s' failed: %w", load, err)
		}
	}
	return loads, nil
}
