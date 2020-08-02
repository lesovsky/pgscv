package collector

import (
	"bufio"
	"bytes"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/prometheus/client_golang/prometheus"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

type systemCollector struct {
	sysctlList []string
	sysctl     typedDesc
	cpucores   typedDesc
	governors  typedDesc
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
		cpucores: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "system", "cpu_cores_total"),
				"Total number of CPU cores in each state.",
				[]string{"state"}, labels,
			), valueType: prometheus.GaugeValue,
		},
		governors: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "system", "scaling_governors_total"),
				"Total number of CPU scaling governors used of each type.",
				[]string{"type"}, labels,
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

	// Count CPU cores by state.
	cpuonline, cpuoffline, err := countCPUCores("/sys/devices/system/cpu/cpu*")
	if err != nil {
		log.Warnf("cpu count failed: %s; skip", err)
	} else {
		ch <- c.cpucores.mustNewConstMetric(cpuonline, "online")
		ch <- c.cpucores.mustNewConstMetric(cpuoffline, "offline")
	}

	governors, err := countScalingGovernors("/sys/devices/system/cpu/cpu*")
	if err != nil {
		log.Warnf("count CPU scaling governors failed: %s; skip", err)
	} else {
		for governor, total := range governors {
			ch <- c.governors.mustNewConstMetric(total, governor)
		}
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

// countCPUCores counts states of CPU cores present in the system.
func countCPUCores(path string) (float64, float64, error) {
	var onlineCnt, offlineCnt float64

	dirs, err := filepath.Glob(path)
	if err != nil {
		return 0, 0, err
	}

	re, err := regexp.Compile(`cpu[0-9]+$`)
	if err != nil {
		return 0, 0, err
	}

	for _, d := range dirs {
		if strings.HasSuffix(d, "/cpu0") { // cpu0 has no 'online' file and always online, just increment counter
			onlineCnt++
			continue
		}

		file := d + "/online"
		if re.MatchString(d) {
			content, err := ioutil.ReadFile(file)
			if err != nil {
				return 0, 0, err
			}
			reader := bufio.NewReader(bytes.NewBuffer(content))
			line, _, err := reader.ReadLine()
			if err != nil {
				return 0, 0, err
			}

			switch string(line) {
			case "0":
				offlineCnt += 1
			case "1":
				onlineCnt += 1
			default:
				log.Warnf("count cpu cores failed, bad value in %s: %s; skip", file, line)
			}
		}
	}
	return onlineCnt, offlineCnt, nil
}

func countScalingGovernors(path string) (map[string]float64, error) {
	re, err := regexp.Compile(`cpu[0-9]+$`)
	if err != nil {
		return nil, err
	}

	dirs, err := filepath.Glob(path)
	if err != nil {
		return nil, err
	}

	var governors = map[string]float64{}

	for _, d := range dirs {
		if !re.MatchString(d) { // skip other than 'cpu*' dirs
			continue
		}

		fi, err := os.Stat(d + "/cpufreq")
		if err != nil {
			continue // cpufreq dir not found -- no cpu scaling used
		}

		if !fi.IsDir() {
			log.Errorf("%s should be a directory; skip", fi.Name())
			continue
		}

		file := d + "/cpufreq" + "/scaling_governor"
		content, err := ioutil.ReadFile(file)
		if err != nil {
			return nil, err
		}
		reader := bufio.NewReader(bytes.NewBuffer(content))
		line, _, err := reader.ReadLine()
		if err != nil {
			return nil, err
		}
		governors[string(line)]++
	}
	return governors, nil
}
