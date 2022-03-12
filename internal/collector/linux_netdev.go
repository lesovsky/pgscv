package collector

import (
	"bufio"
	"fmt"
	"github.com/lesovsky/pgscv/internal/filter"
	"github.com/lesovsky/pgscv/internal/log"
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"os"
	"strconv"
	"strings"
)

type netdevCollector struct {
	bytes   typedDesc
	packets typedDesc
	events  typedDesc
}

// NewNetdevCollector returns a new Collector exposing network interfaces stats.
func NewNetdevCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {

	// Define default filters (if no already present) to avoid collecting metrics about virtual interfaces.
	if _, ok := settings.Filters["device"]; !ok {
		if settings.Filters == nil {
			settings.Filters = filter.New()
		}

		settings.Filters.Add("device", filter.Filter{Exclude: `docker|virbr`})
		err := settings.Filters.Compile()
		if err != nil {
			return nil, err
		}
	}

	return &netdevCollector{
		bytes: newBuiltinTypedDesc(
			descOpts{"node", "network", "bytes_total", "Total number of bytes processed by network device, by each direction.", 0},
			prometheus.CounterValue,
			[]string{"device", "type"}, constLabels,
			settings.Filters,
		),
		packets: newBuiltinTypedDesc(
			descOpts{"node", "network", "packets_total", "Total number of packets processed by network device, by each direction.", 0},
			prometheus.CounterValue,
			[]string{"device", "type"}, constLabels,
			settings.Filters,
		),
		events: newBuiltinTypedDesc(
			descOpts{"node", "network", "events_total", "Total number of events occurred on network device, by each type and direction.", 0},
			prometheus.CounterValue,
			[]string{"device", "type", "event"}, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects network interfaces statistics
func (c *netdevCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
	stats, err := getNetdevStats()
	if err != nil {
		return fmt.Errorf("get /proc/net/dev stats failed: %s", err)
	}

	for device, stat := range stats {
		if len(stat) < 16 {
			log.Warnf("too few stats columns (%d), skip", len(stat))
			continue
		}

		// recv
		ch <- c.bytes.newConstMetric(stat[0], device, "recv")
		ch <- c.packets.newConstMetric(stat[1], device, "recv")
		ch <- c.events.newConstMetric(stat[2], device, "recv", "errs")
		ch <- c.events.newConstMetric(stat[3], device, "recv", "drop")
		ch <- c.events.newConstMetric(stat[4], device, "recv", "fifo")
		ch <- c.events.newConstMetric(stat[5], device, "recv", "frame")
		ch <- c.events.newConstMetric(stat[6], device, "recv", "compressed")
		ch <- c.events.newConstMetric(stat[7], device, "recv", "multicast")

		// sent
		ch <- c.bytes.newConstMetric(stat[8], device, "sent")
		ch <- c.packets.newConstMetric(stat[9], device, "sent")
		ch <- c.events.newConstMetric(stat[10], device, "sent", "errs")
		ch <- c.events.newConstMetric(stat[11], device, "sent", "drop")
		ch <- c.events.newConstMetric(stat[12], device, "sent", "fifo")
		ch <- c.events.newConstMetric(stat[13], device, "sent", "colls")
		ch <- c.events.newConstMetric(stat[14], device, "sent", "carrier")
		ch <- c.events.newConstMetric(stat[15], device, "sent", "compressed")
	}

	return nil
}

// getNetdevStats is the intermediate function which opens stats file and run stats parser for extracting stats.
func getNetdevStats() (map[string][]float64, error) {
	file, err := os.Open("/proc/net/dev")
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	return parseNetdevStats(file)
}

// parseNetdevStats accepts file descriptor, reads file content and produces stats.
func parseNetdevStats(r io.Reader) (map[string][]float64, error) {
	log.Debug("parse network devices stats")

	scanner := bufio.NewScanner(r)

	// Stats file /proc/net/dev has header consisting of two lines. Read the header and check content to make sure this is proper file.
	for i := 0; i < 2; i++ {
		scanner.Scan()
		parts := strings.Split(scanner.Text(), "|")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid input, '%s': wrong number of values", scanner.Text())
		}
	}

	var stats = map[string][]float64{}

	for scanner.Scan() {
		values := strings.Fields(scanner.Text())

		device := strings.TrimRight(values[0], ":")

		// Create float64 slice for values, parse line except first three values (major/minor/device)
		stat := make([]float64, len(values)-1)
		for i := range stat {
			value, err := strconv.ParseFloat(values[i+1], 64)
			if err != nil {
				log.Errorf("invalid input, parse '%s' failed: %s, skip", values[i+1], err.Error())
				continue
			}
			stat[i] = value
		}

		stats[device] = stat
	}

	return stats, scanner.Err()
}
