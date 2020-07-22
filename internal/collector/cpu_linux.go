package collector

import (
	"fmt"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/procfs"
	"sync"
)

type cpuCollector struct {
	fs            procfs.FS
	cpu           *prometheus.Desc
	cpuGuest      *prometheus.Desc
	cpuStats      []procfs.CPUStat // per-CPU stats
	cpuStatsSum   procfs.CPUStat   // summary stats across all CPUs
	cpuStatsMutex sync.Mutex
}

// NewCPUCollector returns a new Collector exposing kernel/system statistics.
func NewCPUCollector(labels prometheus.Labels) (Collector, error) {
	fs, err := procfs.NewFS(procfs.DefaultMountPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to open procfs: %w", err)
	}
	c := &cpuCollector{
		fs: fs,
		cpu: prometheus.NewDesc(
			prometheus.BuildFQName("pgscv", "cpu", "seconds_total"),
			"Seconds the cpus spent in each mode.",
			[]string{"mode"}, labels,
		),
		cpuGuest: prometheus.NewDesc(
			prometheus.BuildFQName("pgscv", "cpu", "guest_seconds_total"),
			"Seconds the cpus spent in guests (VMs) for each mode.",
			[]string{"mode"}, labels,
		),
	}
	return c, nil
}

// Update implements Collector and exposes cpu related metrics from /proc/stat and /sys/.../cpu/.
func (c *cpuCollector) Update(ch chan<- prometheus.Metric) error {
	if err := c.updateStat(ch); err != nil {
		return err
	}
	return nil
}

func (c *cpuCollector) updateStat(ch chan<- prometheus.Metric) error {
	stats, err := c.fs.Stat()
	if err != nil {
		return err
	}

	c.updateCPUStats(stats.CPU)

	// Acquire a lock to read the stats.
	c.cpuStatsMutex.Lock()
	defer c.cpuStatsMutex.Unlock()

	ch <- prometheus.MustNewConstMetric(c.cpu, prometheus.CounterValue, c.cpuStatsSum.User, "user")
	ch <- prometheus.MustNewConstMetric(c.cpu, prometheus.CounterValue, c.cpuStatsSum.Nice, "nice")
	ch <- prometheus.MustNewConstMetric(c.cpu, prometheus.CounterValue, c.cpuStatsSum.System, "system")
	ch <- prometheus.MustNewConstMetric(c.cpu, prometheus.CounterValue, c.cpuStatsSum.Idle, "idle")
	ch <- prometheus.MustNewConstMetric(c.cpu, prometheus.CounterValue, c.cpuStatsSum.Iowait, "iowait")
	ch <- prometheus.MustNewConstMetric(c.cpu, prometheus.CounterValue, c.cpuStatsSum.IRQ, "irq")
	ch <- prometheus.MustNewConstMetric(c.cpu, prometheus.CounterValue, c.cpuStatsSum.SoftIRQ, "softirq")
	ch <- prometheus.MustNewConstMetric(c.cpu, prometheus.CounterValue, c.cpuStatsSum.Steal, "steal")

	// Guest CPU is also accounted for in cpuStat.User and cpuStat.Nice, expose these as separate metrics.
	ch <- prometheus.MustNewConstMetric(c.cpuGuest, prometheus.CounterValue, c.cpuStatsSum.Guest, "user")
	ch <- prometheus.MustNewConstMetric(c.cpuGuest, prometheus.CounterValue, c.cpuStatsSum.GuestNice, "nice")

	return nil
}

// updateCPUStats updates the internal cache of CPU stats.
func (c *cpuCollector) updateCPUStats(newStats []procfs.CPUStat) {
	// Acquire a lock to update the stats.
	c.cpuStatsMutex.Lock()
	defer c.cpuStatsMutex.Unlock()

	// Reset the cache if the list of CPUs has changed.
	if len(c.cpuStats) != len(newStats) {
		c.cpuStats = make([]procfs.CPUStat, len(newStats))
	}

	// Сначала собирем per-CPU статистику, если Idle для какого-то из ядер скакнуло назад то мы сбрасываем его стату, не сбрасывая при этом стату для других ядер.
	// После того как стата собрана можем ее агрегировать

	// update current snapshot of CPU stats, skip those counters who jumped backwards
	for i, n := range newStats {
		// If idle jumps backwards, assume we had a hotplug event and reset the stats for this CPU.
		if n.Idle < c.cpuStats[i].Idle {
			log.Warnln("CPU Idle counter jumped backwards, possible hotplug event, resetting CPU stats", "cpu", i, "old_value", c.cpuStats[i].Idle, "new_value", n.Idle)
			c.cpuStats[i] = procfs.CPUStat{}
		}
		c.cpuStats[i].Idle = n.Idle

		if n.User >= c.cpuStats[i].User {
			c.cpuStats[i].User = n.User
		} else {
			log.Warnln("CPU User counter jumped backwards", "cpu", i, "old_value", c.cpuStats[i].User, "new_value", n.User)
		}

		if n.Nice >= c.cpuStats[i].Nice {
			c.cpuStats[i].Nice = n.Nice
		} else {
			log.Warnln("CPU Nice counter jumped backwards", "cpu", i, "old_value", c.cpuStats[i].Nice, "new_value", n.Nice)
		}

		if n.System >= c.cpuStats[i].System {
			c.cpuStats[i].System = n.System
		} else {
			log.Warnln("CPU System counter jumped backwards", "cpu", i, "old_value", c.cpuStats[i].System, "new_value", n.System)
		}

		if n.Iowait >= c.cpuStats[i].Iowait {
			c.cpuStats[i].Iowait = n.Iowait
		} else {
			log.Warnln("CPU Iowait counter jumped backwards", "cpu", i, "old_value", c.cpuStats[i].Iowait, "new_value", n.Iowait)
		}

		if n.IRQ >= c.cpuStats[i].IRQ {
			c.cpuStats[i].IRQ = n.IRQ
		} else {
			log.Warnln("CPU IRQ counter jumped backwards", "cpu", i, "old_value", c.cpuStats[i].IRQ, "new_value", n.IRQ)
		}

		if n.SoftIRQ >= c.cpuStats[i].SoftIRQ {
			c.cpuStats[i].SoftIRQ = n.SoftIRQ
		} else {
			log.Warnln("CPU SoftIRQ counter jumped backwards", "cpu", i, "old_value", c.cpuStats[i].SoftIRQ, "new_value", n.SoftIRQ)
		}

		if n.Steal >= c.cpuStats[i].Steal {
			c.cpuStats[i].Steal = n.Steal
		} else {
			log.Warnln("CPU Steal counter jumped backwards", "cpu", i, "old_value", c.cpuStats[i].Steal, "new_value", n.Steal)
		}

		if n.Guest >= c.cpuStats[i].Guest {
			c.cpuStats[i].Guest = n.Guest
		} else {
			log.Warnln("CPU Guest counter jumped backwards", "cpu", i, "old_value", c.cpuStats[i].Guest, "new_value", n.Guest)
		}

		if n.GuestNice >= c.cpuStats[i].GuestNice {
			c.cpuStats[i].GuestNice = n.GuestNice
		} else {
			log.Warnln("CPU GuestNice counter jumped backwards", "cpu", i, "old_value", c.cpuStats[i].GuestNice, "new_value", n.GuestNice)
		}
	}

	// Produce aggregated CPU stats based on updated local snapshot.
	c.cpuStatsSum = procfs.CPUStat{}
	for _, n := range newStats {
		c.cpuStatsSum.Idle += n.Idle
		c.cpuStatsSum.User += n.User
		c.cpuStatsSum.Nice += n.Nice
		c.cpuStatsSum.System += n.System
		c.cpuStatsSum.Iowait += n.Iowait
		c.cpuStatsSum.IRQ += n.IRQ
		c.cpuStatsSum.SoftIRQ += n.SoftIRQ
		c.cpuStatsSum.Steal += n.Steal
		c.cpuStatsSum.Guest += n.Guest
		c.cpuStatsSum.GuestNice += n.GuestNice
	}
}
