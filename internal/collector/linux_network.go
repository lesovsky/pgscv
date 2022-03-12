package collector

import (
	"fmt"
	"github.com/lesovsky/pgscv/internal/log"
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"net"
	"strings"
)

type networkCollector struct {
	privateAddresses typedDesc
	publicAddresses  typedDesc
}

func NewNetworkCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &networkCollector{
		publicAddresses: newBuiltinTypedDesc(
			descOpts{"node", "network", "public_addresses", "Number of public network addresses present on the system, by type.", 0},
			prometheus.GaugeValue,
			nil, constLabels,
			settings.Filters,
		),
		privateAddresses: newBuiltinTypedDesc(
			descOpts{"node", "network", "private_addresses", "Number of private network addresses present on the system, by type.", 0},
			prometheus.GaugeValue,
			nil, constLabels,
			settings.Filters,
		),
	}, nil
}

func (c *networkCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return err
	}

	stats := parseInterfaceAddresses(addresses)

	ch <- c.publicAddresses.newConstMetric(float64(stats["public"]))
	ch <- c.privateAddresses.newConstMetric(float64(stats["private"]))

	return nil
}

func parseInterfaceAddresses(addresses []net.Addr) map[string]int {
	log.Debug("parse network addresses")
	addrByType := map[string]int{
		"private": 0,
		"public":  0,
	}

	for _, addr := range addresses {
		private, err := isPrivate(addr.String())
		if err != nil {
			log.Warnf("invalid input, parse '%s' failed: %s, skip", addr.String(), err)
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
