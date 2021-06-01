package collector

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/filter"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"os"
	"strconv"
	"strings"
)

type loadaverageCollector struct {
	load1  typedDesc
	load5  typedDesc
	load15 typedDesc
}

// NewLoadAverageCollector returns a new Collector exposing load average statistics.
func NewLoadAverageCollector(labels prometheus.Labels, _ model.CollectorSettings) (Collector, error) {
	return &loadaverageCollector{
		load1: newBuiltinTypedDesc(
			descOpts{"node", "", "load1", "1m load average.", 0},
			prometheus.GaugeValue,
			nil, labels,
			filter.New(),
		),
		load5: newBuiltinTypedDesc(
			descOpts{"node", "", "load5", "5m load average.", 0},
			prometheus.GaugeValue,
			nil, labels,
			filter.New(),
		),
		load15: newBuiltinTypedDesc(
			descOpts{"node", "", "load15", "15m load average.", 0},
			prometheus.GaugeValue,
			nil, labels,
			filter.New(),
		),
	}, nil
}

// Update implements Collector and exposes load average related metrics from /proc/loadavg.
func (c *loadaverageCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
	stats, err := getLoadAverageStats()
	if err != nil {
		return fmt.Errorf("get load average stats failed: %s", err)
	}

	ch <- c.load1.newConstMetric(stats[0])
	ch <- c.load5.newConstMetric(stats[1])
	ch <- c.load15.newConstMetric(stats[2])

	return nil
}

// getLoadAverageStats reads /proc/loadavg and return load stats.
func getLoadAverageStats() ([]float64, error) {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return nil, err
	}

	return parseLoadAverageStats(string(data))
}

// parseLoadAverageStats parses content from /proc/loadavg and return load stats.
func parseLoadAverageStats(data string) ([]float64, error) {
	log.Debug("parse load average stats")

	parts := strings.Fields(data)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid input, '%s': too few values", data)
	}

	var err error
	loads := make([]float64, 3)
	for i, load := range parts[0:3] {
		loads[i], err = strconv.ParseFloat(load, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid input, parse '%s' failed: %w", load, err)
		}
	}
	return loads, nil
}
