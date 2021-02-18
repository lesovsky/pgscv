package collector

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"io"
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
	numanodes  typedDesc
	ctxt       typedDesc
	forks      typedDesc
	btime      typedDesc
}

// NewSystemCollector returns a new Collector exposing system-wide stats.
func NewSysconfigCollector(labels prometheus.Labels) (Collector, error) {
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
				[]string{"governor"}, labels,
			), valueType: prometheus.GaugeValue,
		},
		numanodes: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "system", "numa_nodes_total"),
				"Total number of NUMA nodes in the system.",
				nil, labels,
			), valueType: prometheus.GaugeValue,
		},
		ctxt: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "", "context_switches_total"),
				"Total number of context switches.",
				nil, labels,
			), valueType: prometheus.CounterValue,
		},
		forks: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "", "forks_total"),
				"Total number of forks.",
				nil, labels,
			), valueType: prometheus.CounterValue,
		},
		btime: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "", "boot_time_seconds"),
				"Node boot time, in unixtime.",
				nil, labels,
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

	// Count CPU scaling governors.
	governors, err := countScalingGovernors("/sys/devices/system/cpu/cpu*")
	if err != nil {
		log.Warnf("count CPU scaling governors failed: %s; skip", err)
	} else {
		for governor, total := range governors {
			ch <- c.governors.mustNewConstMetric(total, governor)
		}
	}

	// Count NUMA nodes.
	nodes, err := countNumaNodes("/sys/devices/system/node/node*")
	if err != nil {
		log.Warnf("count NUMA nodes failed: %s; skip", err)
	} else {
		ch <- c.numanodes.mustNewConstMetric(nodes)
	}

	// Collect /proc/stat based metrics.
	stat, err := getProcStat()
	if err != nil {
		log.Warnf("parse /proc/stat failed: %s; skip", err)
	} else {
		ch <- c.ctxt.mustNewConstMetric(stat.ctxt)
		ch <- c.btime.mustNewConstMetric(stat.btime)
		ch <- c.forks.mustNewConstMetric(stat.forks)
	}

	return nil
}

// readSysctls reads list of passed sysctls and return map with its names and values.
func readSysctls(list []string) map[string]float64 {
	var sysctls = map[string]float64{}
	for _, item := range list {
		data, err := os.ReadFile(path.Join("/proc/sys", strings.Replace(item, ".", "/", -1)))
		if err != nil {
			log.Warnf("read '%s' failed: %s; skip", item, err)
			continue
		}
		value, err := strconv.ParseFloat(strings.Trim(string(data), " \n"), 64)
		if err != nil {
			log.Warnf("invalid input, parse '%s' failed: %s; skip", item, err)
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
			content, err := os.ReadFile(filepath.Clean(file))
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
				offlineCnt++
			case "1":
				onlineCnt++
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
		content, err := os.ReadFile(filepath.Clean(file))
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

// countNumaNodes counts NUMA nodes in the system.
func countNumaNodes(path string) (n float64, err error) {
	d, err := filepath.Glob(path)
	if err != nil {
		return 0, err
	}
	return float64(len(d)), nil
}

// systemProcStat represents some stats from /proc/stat file.
type systemProcStat struct {
	ctxt  float64
	btime float64
	forks float64
}

func getProcStat() (systemProcStat, error) {
	file, err := os.Open("/proc/stat")
	if err != nil {
		return systemProcStat{}, err
	}
	defer func() { _ = file.Close() }()

	return parseProcStat(file)
}

func parseProcStat(r io.Reader) (systemProcStat, error) {
	log.Debug("parse system stats")
	var (
		scanner = bufio.NewScanner(r)
		stat    = systemProcStat{}
		err     error
	)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) < 2 {
			log.Debugf("invalid input, '%s': too few values; skip", line)
			continue
		}

		switch parts[0] {
		case "ctxt":
			stat.ctxt, err = strconv.ParseFloat(parts[1], 64)
			if err != nil {
				return stat, fmt.Errorf("invalid input, parse '%s' (ctxt) failed: %s; skip", parts[1], err)
			}
		case "btime":
			stat.btime, err = strconv.ParseFloat(parts[1], 64)
			if err != nil {
				return stat, fmt.Errorf("invalid input, parse '%s' (btime) failed: %s; skip", parts[1], err)
			}
		case "processes":
			stat.forks, err = strconv.ParseFloat(parts[1], 64)
			if err != nil {
				return stat, fmt.Errorf("invalid input, parse '%s' (processes) failed: %s; skip", parts[1], err)
			}
		default:
			continue
		}
	}

	return stat, nil
}
