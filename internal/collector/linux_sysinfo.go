package collector

import (
	"bufio"
	"fmt"
	"github.com/lesovsky/pgscv/internal/log"
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type sysinfoCollector struct {
	platform typedDesc
	os       typedDesc
}

// NewSysInfoCollector returns a new Collector exposing system info.
func NewSysInfoCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &sysinfoCollector{
		platform: newBuiltinTypedDesc(
			descOpts{"node", "platform", "info", "Labeled system platform information", 0},
			prometheus.GaugeValue,
			[]string{"vendor", "product_name"}, constLabels,
			settings.Filters,
		),
		os: newBuiltinTypedDesc(
			descOpts{"node", "os", "info", "Labeled operating system information.", 0},
			prometheus.GaugeValue,
			[]string{"kernel", "type", "name", "version"}, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update implements Collector and exposes system info metrics.
func (c *sysinfoCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
	info, err := getSysInfo()
	if err != nil {
		return err
	}

	ch <- c.platform.newConstMetric(1, info.sysVendor, info.productName)
	ch <- c.os.newConstMetric(1, info.kernel, info.osType, info.osName, info.osVersion)

	return nil
}

// sysInfo contains various information about platform and operating system.
type sysInfo struct {
	sysVendor   string
	productName string
	kernel      string
	osType      string
	osName      string
	osVersion   string
}

// getSysInfo reads various information about platform and system.
func getSysInfo() (*sysInfo, error) {
	vendor, err := os.ReadFile("/sys/class/dmi/id/sys_vendor")
	if err != nil {
		return nil, err
	}

	name, err := os.ReadFile("/sys/class/dmi/id/product_name")
	if err != nil {
		return nil, err
	}

	kernel, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		return nil, err
	}

	osType, err := os.ReadFile("/proc/sys/kernel/ostype")
	if err != nil {
		return nil, err
	}

	osName, osVersion, err := getOsRelease()
	if err != nil {
		return nil, err
	}

	return &sysInfo{
		sysVendor:   strings.TrimSpace(string(vendor)),
		productName: strings.TrimSpace(string(name)),
		kernel:      strings.TrimSpace(string(kernel)),
		osType:      strings.TrimSpace(string(osType)),
		osName:      osName,
		osVersion:   osVersion,
	}, nil
}

// getOsRelease reads content of /etc/os-release and returns OS name and version.
func getOsRelease() (string, string, error) {
	file, err := os.Open(filepath.Clean("/etc/os-release"))
	if err != nil {
		return "", "", err
	}
	defer func() { _ = file.Close() }()

	return parseOsRelease(file)
}

// parseOsRelease scans buffer data for OS name and version.
func parseOsRelease(r io.Reader) (string, string, error) {
	log.Debug("parse os-release info")

	scanner := bufio.NewScanner(r)
	var name, version string

	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "=", 2)

		if len(parts) != 2 {
			return "", "", fmt.Errorf("invalid input, '%s': wrong number of values", scanner.Text())
		}

		key, value := parts[0], parts[1]

		switch key {
		case "NAME":
			name = strings.Trim(value, `"`)
		case "VERSION":
			version = strings.Trim(value, `"`)
		default:
			continue
		}
	}

	return name, version, scanner.Err()
}
