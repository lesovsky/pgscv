package collector

import (
	"bufio"
	"fmt"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	netdevIgnoredDevicePattern = `docker|virbr`
)

type netdevCollector struct {
	ignoredDevicePattern *regexp.Regexp
	bytes                typedDesc
	packets              typedDesc
	events               typedDesc
}

// NewNetdevCollector returns a new Collector exposing network interfaces stats.
func NewNetdevCollector(labels prometheus.Labels) (Collector, error) {
	return &netdevCollector{
		ignoredDevicePattern: regexp.MustCompile(netdevIgnoredDevicePattern),
		bytes: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "network", "bytes_total"),
				"Total number of bytes processed by network device, by each direction.",
				[]string{"device", "type"}, labels,
			), valueType: prometheus.CounterValue,
		},
		packets: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "network", "packets_total"),
				"Total number of packets processed by network device, by each direction.",
				[]string{"device", "type"}, labels,
			), valueType: prometheus.CounterValue,
		},
		events: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "network", "events_total"),
				"Total number of events occurred on network device, by each type and direction.",
				[]string{"device", "type", "event"}, labels,
			), valueType: prometheus.CounterValue,
		},
	}, nil
}

// Update method collects network interfaces statistics
func (c *netdevCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
	stats, err := getNetdevStats(c.ignoredDevicePattern)
	if err != nil {
		return fmt.Errorf("get /proc/net/dev stats failed: %s", err)
	}

	for _, s := range stats {
		// bytes received/sent
		ch <- c.bytes.mustNewConstMetric(s.rbytes, s.device, "recv")
		ch <- c.bytes.mustNewConstMetric(s.tbytes, s.device, "sent")
		// packets received/sent
		ch <- c.packets.mustNewConstMetric(s.rpackets, s.device, "recv")
		ch <- c.packets.mustNewConstMetric(s.tpackets, s.device, "sent")
		// events (errors/drops occurred)
		ch <- c.events.mustNewConstMetric(s.rerrs, s.device, "recv", "errs")
		ch <- c.events.mustNewConstMetric(s.rdrop, s.device, "recv", "drop")
		ch <- c.events.mustNewConstMetric(s.rfifo, s.device, "recv", "fifo")
		ch <- c.events.mustNewConstMetric(s.rframe, s.device, "recv", "frame")
		ch <- c.events.mustNewConstMetric(s.rcompressed, s.device, "recv", "compressed")
		ch <- c.events.mustNewConstMetric(s.rmulticast, s.device, "recv", "multicast")
		ch <- c.events.mustNewConstMetric(s.terrs, s.device, "sent", "errs")
		ch <- c.events.mustNewConstMetric(s.tdrop, s.device, "sent", "drop")
		ch <- c.events.mustNewConstMetric(s.tfifo, s.device, "sent", "fifo")
		ch <- c.events.mustNewConstMetric(s.tcolls, s.device, "sent", "colls")
		ch <- c.events.mustNewConstMetric(s.tcarrier, s.device, "sent", "carrier")
		ch <- c.events.mustNewConstMetric(s.tcompressed, s.device, "sent", "compressed")
	}

	return nil
}

// netdevStat represents network devices stats from /proc/net/dev.
type netdevStat struct {
	device      string
	rbytes      float64
	rpackets    float64
	rerrs       float64
	rdrop       float64
	rfifo       float64
	rframe      float64
	rcompressed float64
	rmulticast  float64
	tbytes      float64
	tpackets    float64
	terrs       float64
	tdrop       float64
	tfifo       float64
	tcolls      float64
	tcarrier    float64
	tcompressed float64
}

// getNetdevStats is the intermediate function which opens stats file and run stats parser for extracting stats.
func getNetdevStats(ignore *regexp.Regexp) ([]netdevStat, error) {
	file, err := os.Open(filepath.Clean("/proc/net/dev"))
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	return parseNetdevStats(file, ignore)
}

// parseNetdevStats accepts file descriptor, reads file content and produces stats.
func parseNetdevStats(r io.Reader, ignore *regexp.Regexp) ([]netdevStat, error) {
	scanner := bufio.NewScanner(r)

	// Stats file /proc/net/dev has header consisting of two lines. Read the header and check content to make sure this is proper file.
	for i := 0; i < 2; i++ {
		scanner.Scan()
		parts := strings.Split(scanner.Text(), "|")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid header line in /proc/net/dev: %s", scanner.Text())
		}
	}

	var stats []netdevStat

	for scanner.Scan() {
		// Read the line and looking for device name and stats values
		line := strings.TrimLeft(scanner.Text(), " ")

		var s = netdevStat{}

		_, err := fmt.Sscanln(line,
			&s.device,
			&s.rbytes, &s.rpackets, &s.rerrs, &s.rdrop, &s.rfifo, &s.rframe, &s.rcompressed, &s.rmulticast,
			&s.tbytes, &s.tpackets, &s.terrs, &s.tdrop, &s.tfifo, &s.tcolls, &s.tcarrier, &s.tcompressed)
		if err != nil {
			log.Errorf("scan stats from /proc/net/dev failed: %s; skip", err)
			continue
		}

		if ignore != nil && ignore.MatchString(s.device) {
			log.Debugln("ignore device ", s.device)
			continue
		}

		s.device = strings.TrimRight(s.device, ":")

		stats = append(stats, s)
	}

	return stats, scanner.Err()
}
