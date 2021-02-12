package collector

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"net"
	"strings"
)

type networkCollector struct {
	privateAddresses typedDesc
	publicAddresses  typedDesc
}

func NewNetworkCollector(labels prometheus.Labels) (Collector, error) {
	return &networkCollector{
		publicAddresses: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "network", "public_addresses"),
				"Number of public network addresses present on the system, by type.",
				nil, labels,
			), valueType: prometheus.GaugeValue,
		},
		privateAddresses: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "network", "private_addresses"),
				"Number of private network addresses present on the system, by type.",
				nil, labels,
			), valueType: prometheus.GaugeValue,
		},
	}, nil
}

func (c *networkCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return err
	}

	stats := parseInterfaceAddresses(addresses)

	ch <- c.publicAddresses.mustNewConstMetric(float64(stats["public"]))
	ch <- c.privateAddresses.mustNewConstMetric(float64(stats["private"]))

	return nil
}

func parseInterfaceAddresses(addresses []net.Addr) map[string]int {
	addrByType := map[string]int{
		"private": 0,
		"public":  0,
	}

	for _, addr := range addresses {
		private, err := isPrivate(addr.String())
		if err != nil {
			log.Warnf("failed parse network address: %s; skip", err)
			continue
		}

		if private {
			addrByType["private"]++
		} else {
			addrByType["public"]++
		}
	}

	return addrByType
}

func isPrivate(address string) (bool, error) {
	networks := []string{
		"127.0.0.0/8",    // IPv4 loopback
		"10.0.0.0/8",     // RFC1918
		"172.16.0.0/12",  // RFC1918
		"192.168.0.0/16", // RFC1918
		"::1/128",        // IPv6 loopback
		"fe80::/10",      // IPv6 link-local
		"fc00::/7",       // IPv6 unique-local
	}

	for _, cidr := range networks {
		_, conv, err := net.ParseCIDR(cidr)
		if err != nil {
			return false, err
		}

		address = strings.Split(address, "/")[0]
		ip := net.ParseIP(address)
		if ip == nil {
			return false, fmt.Errorf("invalid ip address: %s", address)
		}

		if conv.Contains(ip) {
			return true, nil
		}
	}
	return false, nil
}
