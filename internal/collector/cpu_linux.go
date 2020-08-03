package collector

import (
	"bufio"
	"fmt"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

type cpuCollector struct {
	systicks float64
	cpu      typedDesc
	cpuGuest typedDesc
}

// NewCPUCollector returns a new Collector exposing kernel/system statistics.
func NewCPUCollector(labels prometheus.Labels) (Collector, error) {
	cmdOutput, err := exec.Command("getconf", "CLK_TCK").Output()
	if err != nil {
		return nil, fmt.Errorf("determine clock frequency failed: %s", err)
	}

	systicks, err := strconv.ParseFloat(strings.TrimSpace(string(cmdOutput)), 64)
	if err != nil {
		return nil, fmt.Errorf("parse clock frequency value failed: %s", err)
	}

	c := &cpuCollector{
		systicks: systicks,
		cpu: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "cpu", "seconds_total"),
				"Seconds the cpus spent in each mode.",
				[]string{"mode"}, labels,
			),
			valueType: prometheus.CounterValue,
		},
		cpuGuest: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("node", "cpu", "guest_seconds_total"),
				"Seconds the cpus spent in guests (VMs) for each mode.",
				[]string{"mode"}, labels,
			),
			valueType: prometheus.CounterValue,
		},
	}
	return c, nil
}

// Update implements Collector and exposes cpu related metrics from /proc/stat and /sys/.../cpu/.
func (c *cpuCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
	stat, err := getCPUStat(c.systicks)
	if err != nil {
		return fmt.Errorf("collect cpu usage stats failed: %s; skip", err)
	}

	ch <- c.cpu.mustNewConstMetric(stat.user, "user")
	ch <- c.cpu.mustNewConstMetric(stat.nice, "nice")
	ch <- c.cpu.mustNewConstMetric(stat.system, "system")
	ch <- c.cpu.mustNewConstMetric(stat.idle, "idle")
	ch <- c.cpu.mustNewConstMetric(stat.iowait, "iowait")
	ch <- c.cpu.mustNewConstMetric(stat.irq, "irq")
	ch <- c.cpu.mustNewConstMetric(stat.softirq, "softirq")
	ch <- c.cpu.mustNewConstMetric(stat.steal, "steal")

	// Guest CPU is also accounted for in cpuStat.User and cpuStat.Nice, expose these as separate metrics.
	ch <- c.cpuGuest.mustNewConstMetric(stat.guest, "user")
	ch <- c.cpuGuest.mustNewConstMetric(stat.guestnice, "nice")

	return nil
}

// systemProcStatCPU ...
type cpuStat struct {
	user      float64
	nice      float64
	system    float64
	idle      float64
	iowait    float64
	irq       float64
	softirq   float64
	steal     float64
	guest     float64
	guestnice float64
}

// getCPUStat opens stat file and executes parser.
func getCPUStat(systicks float64) (cpuStat, error) {
	file, err := os.Open("/proc/stat")
	if err != nil {
		return cpuStat{}, err
	}
	defer func() { _ = file.Close() }()

	return parseProcCPUStat(file, systicks)
}

// parseProcCPUStat parses stat file and returns total CPU usage stat.
func parseProcCPUStat(r io.Reader, systicks float64) (cpuStat, error) {
	var scanner = bufio.NewScanner(r)

	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())
		if len(parts) < 2 {
			log.Debugf("/proc/stat bad line; skip")
			continue
		}

		// Looking only for total stat. We're not interested in per-CPU stats.
		if parts[0] != "cpu" {
			continue
		}

		return parseCPUStat(scanner.Text(), systicks)
	}

	return cpuStat{}, fmt.Errorf("total cpu stats not found")
}

// parseCPUStat parses single line from stats file and returns parsed stats.
func parseCPUStat(line string, systicks float64) (cpuStat, error) {
	s := cpuStat{}
	var cpu string

	count, err := fmt.Sscanf(
		line,
		"%s %f %f %f %f %f %f %f %f %f %f",
		&cpu, &s.user, &s.nice, &s.system, &s.idle, &s.iowait, &s.irq, &s.softirq, &s.steal, &s.guest, &s.guestnice,
	)

	if err != nil && err != io.EOF {
		return cpuStat{}, fmt.Errorf("parse %s (cpu) failed: %s", line, err)
	}
	if count != 11 {
		return cpuStat{}, fmt.Errorf("parse %s (cpu) failed: insufficient elements parsed", line)
	}

	s.user /= systicks
	s.nice /= systicks
	s.system /= systicks
	s.idle /= systicks
	s.iowait /= systicks
	s.irq /= systicks
	s.softirq /= systicks
	s.steal /= systicks
	s.guest /= systicks
	s.guestnice /= systicks

	return s, nil
}
