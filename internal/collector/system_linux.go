package collector

import (
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/prometheus/client_golang/prometheus"
	"io/ioutil"
	"path"
	"strconv"
	"strings"
)

type systemCollector struct {
	sysctlList []string
	sysctl     typedDesc
}

// NewSystemCollector returns a new Collector exposing system-wide stats.
func NewSystemCollector(labels prometheus.Labels) (Collector, error) {
	return &systemCollector{
		sysctlList: []string{
			"kernel.sched_migration_cost_ns",
			"kernel.sched_autogroup_enabled",
			"vm.dirty_background_bytes",
			"vm.dirty_bytes",
			"vm.overcommit_memory",
			"vm.overcommit_ratio",
			"vm.swappiness",
			"vm.min_free_kbytes",
			"vm.zone_reclaim_mode",
			"kernel.numa_balancing",
			"vm.nr_hugepages",
			"vm.nr_overcommit_hugepages",
		},
		sysctl: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "system", "sysctl"),
				"Node sysctl system settings.",
				[]string{"sysctl"}, labels,
			), valueType: prometheus.GaugeValue,
		},
	}, nil
}

// Update method collects filesystem usage statistics.
func (c *systemCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
	sysctls := readSysctls(c.sysctlList)

	for name, value := range sysctls {
		ch <- c.sysctl.mustNewConstMetric(value, name)
	}

	return nil
}

// readSysctls reads list of passed sysctls and return map with its names and values.
func readSysctls(list []string) map[string]float64 {
	var sysctls = map[string]float64{}
	for _, item := range list {
		data, err := ioutil.ReadFile(path.Join("/proc/sys", strings.Replace(item, ".", "/", -1)))
		if err != nil {
			log.Warnf("read sysctl %s failed: %s; skip", item, err)
			continue
		}
		value, err := strconv.ParseFloat(strings.Trim(string(data), " \n"), 64)
		if err != nil {
			log.Warnf("parse sysctl %s value failed: %s; skip", item, err)
			continue
		}

		sysctls[item] = value
	}
	return sysctls
}
