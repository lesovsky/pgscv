package collector

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/filter"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

type cpuCollector struct {
	systicks float64
	cpu      typedDesc
	cpuAll   typedDesc
	cpuGuest typedDesc
	uptime   typedDesc
	idletime typedDesc
}

// NewCPUCollector returns a new Collector exposing kernel/system statistics.
func NewCPUCollector(constLabels labels, _ model.CollectorSettings) (Collector, error) {
	cmdOutput, err := exec.Command("getconf", "CLK_TCK").Output()
	if err != nil {
		return nil, fmt.Errorf("determine clock frequency failed: %s", err)
	}

	value := strings.TrimSpace(string(cmdOutput))
	systicks, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid input: parse '%s' failed: %w", value, err)
	}

	c := &cpuCollector{
		systicks: systicks,
		cpu: newBuiltinTypedDesc(
			descOpts{"node", "cpu", "seconds_total", "Seconds the CPUs spent in each mode.", 0},
			prometheus.CounterValue,
			[]string{"mode"}, constLabels,
			filter.New(),
		),
		cpuAll: newBuiltinTypedDesc(
			descOpts{"node", "cpu", "seconds_all_total", "Seconds the CPUs spent in all modes.", 0},
			prometheus.CounterValue,
			nil, constLabels,
			filter.New(),
		),
		cpuGuest: newBuiltinTypedDesc(
			descOpts{"node", "cpu", "guest_seconds_total", "Seconds the CPUs spent in guests (VMs) for each mode.", 0},
			prometheus.CounterValue,
			[]string{"mode"}, constLabels,
			filter.New(),
		),
		uptime: newBuiltinTypedDesc(
			descOpts{"node", "uptime", "up_seconds_total", "Total number of seconds the system has been up, accordingly to /proc/uptime.", 0},
			prometheus.CounterValue,
			nil, constLabels,
			filter.New(),
		),
		idletime: newBuiltinTypedDesc(
			descOpts{"node", "uptime", "idle_seconds_total", "Total number of seconds all cores have spent idle, accordingly to /proc/uptime.", 0},
			prometheus.CounterValue,
			nil, constLabels,
			filter.New(),
		),
	}
	return c, nil
}

// Update implements Collector and exposes cpu related metrics from /proc/stat and /sys/.../cpu/.
func (c *cpuCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
	stat, err := getCPUStat(c.systicks)
	if err != nil {
		return fmt.Errorf("collect cpu usage stats failed: %s; skip", err)
	}

	uptime, idletime, err := getProcUptime("/proc/uptime")
	if err != nil {
		return fmt.Errorf("collect uptime stats failed: %s; skip", err)
	}

	// Collected time represents summary time spent by ALL cpu cores.
	ch <- c.cpu.newConstMetric(stat.user, "user")
	ch <- c.cpu.newConstMetric(stat.nice, "nice")
	ch <- c.cpu.newConstMetric(stat.system, "system")
	ch <- c.cpu.newConstMetric(stat.idle, "idle")
	ch <- c.cpu.newConstMetric(stat.iowait, "iowait")
	ch <- c.cpu.newConstMetric(stat.irq, "irq")
	ch <- c.cpu.newConstMetric(stat.softirq, "softirq")
	ch <- c.cpu.newConstMetric(stat.steal, "steal")

	ch <- c.cpuAll.newConstMetric(stat.user + stat.nice + stat.system + stat.idle + stat.iowait + stat.irq + stat.softirq + stat.steal)

	// Guest CPU is also accounted for in stat.user and stat.nice, expose these as separate metrics.
	ch <- c.cpuGuest.newConstMetric(stat.guest, "user")
	ch <- c.cpuGuest.newConstMetric(stat.guestnice, "nice")

	// Up and idle time values from /proc/uptime. Idle time accounted as summary for all cpu cores.
	ch <- c.uptime.newConstMetric(uptime)
	ch <- c.idletime.newConstMetric(idletime)

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
	log.Debug("parse CPU stats")

	var scanner = bufio.NewScanner(r)

	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())
		if len(parts) < 2 {
			log.Debug("CPU stat invalid input: too few values; skip")
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
		return cpuStat{}, fmt.Errorf("invalid input, parse '%s' failed: %w", line, err)
	}
	if count != 11 {
		return cpuStat{}, fmt.Errorf("invalid input, parse '%s' failed: wrong number of values", line)
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

// getProcUptime parses uptime file (e.g. /proc/uptime) and return uptime and idletime values.
func getProcUptime(procfile string) (float64, float64, error) {
	content, err := os.ReadFile(filepath.Clean(procfile))
	if err != nil {
		return 0, 0, err
	}

	reader := bufio.NewReader(bytes.NewBuffer(content))

	line, _, err := reader.ReadLine()
	if err != nil {
		return 0, 0, err
	}

	var up, idle float64
	_, err = fmt.Sscanf(string(line), "%f %f", &up, &idle)
	if err != nil {
		return 0, 0, err
	}

	return up, idle, nil
}
