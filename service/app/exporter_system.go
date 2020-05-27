package app

import (
	"github.com/barcodepro/pgscv/service/internal/log"
	"github.com/barcodepro/pgscv/service/internal/stat"
	"github.com/barcodepro/pgscv/service/model"
	"github.com/prometheus/client_golang/prometheus"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const (
	// regexp describes raw block devices except their partitions, but including stacked devices, such as device-mapper and mdraid
	regexpBlockDevicesExtended = `((s|xv|v)d[a-z])|(nvme[0-9]n[0-9])|(dm-[0-9]+)|(md[0-9]+)`
)

// collectSystemMetrics is the wrapper for all system metrics collectors
func (e *prometheusExporter) collectSystemMetrics(ch chan<- prometheus.Metric) (cnt int) {
	funcs := map[string]func(chan<- prometheus.Metric) int{
		"node_cpu_usage":                   e.collectCPUMetrics,
		"node_diskstats":                   e.collectDiskstatsMetrics,
		"node_netdev":                      e.collectNetdevMetrics,
		"node_memory":                      e.collectMemMetrics,
		"node_filesystem":                  e.collectFsMetrics,
		"node_settings":                    e.collectSysctlMetrics,
		"node_hardware_cores":              e.collectCPUCoresState,
		"node_hardware_scaling_governors":  e.collectCPUScalingGovernors,
		"node_hardware_numa":               e.collectNumaNodes,
		"node_hardware_storage_rotational": e.collectStorageSchedulers,
		"node_uptime_seconds":              e.collectSystemUptime,
	}

	for i, desc := range e.statCatalog {
		if desc.StatType != model.ServiceTypeSystem {
			continue
		}
		if !desc.IsDescriptorActive() {
			continue
		}
		// execute the method and remember execution time
		cnt += funcs[desc.Name](ch)
		e.statCatalog[i].LastFired = time.Now()
	}
	return cnt
}

// collectCPUMetrics collects CPU usage metrics
func (e *prometheusExporter) collectCPUMetrics(ch chan<- prometheus.Metric) (cnt int) {
	var cpuStat stat.CPURawstat
	cpuStat.ReadLocal()
	for _, mode := range []string{"user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal", "guest", "guest_nice", "total"} {
		ch <- prometheus.MustNewConstMetric(e.AllDesc["node_cpu_usage_time"], prometheus.CounterValue, cpuStat.SingleStat(mode), mode)
		cnt++
	}
	return cnt
}

// collectMemMetrics collects memory/swap usage metrics
func (e *prometheusExporter) collectMemMetrics(ch chan<- prometheus.Metric) (cnt int) {
	var meminfoStat stat.Meminfo
	var usages = []string{"mem_total", "mem_free", "mem_used", "swap_total", "swap_free", "swap_used", "mem_cached", "mem_dirty",
		"mem_writeback", "mem_buffers", "mem_available", "mem_slab", "hp_total", "hp_free", "hp_rsvd", "hp_surp", "hp_pagesize"}
	meminfoStat.ReadLocal()
	for _, usage := range usages {
		ch <- prometheus.MustNewConstMetric(e.AllDesc["node_memory_usage_bytes"], prometheus.GaugeValue, float64(meminfoStat.SingleStat(usage)), usage)
		cnt++
	}
	return cnt
}

// collectDiskstatsMetrics collects block devices usage metrics
func (e *prometheusExporter) collectDiskstatsMetrics(ch chan<- prometheus.Metric) (cnt int) {
	var diskUtilStat stat.Diskstats
	bdevCnt, err := stat.CountLinesLocal(stat.ProcDiskstats)
	if err == nil {
		diskUtilStat = make(stat.Diskstats, bdevCnt)
		err := diskUtilStat.ReadLocal()
		if err != nil {
			log.Errorln("failed to collect diskstats metrics: ", err)
			return 0
		}

		for _, s := range diskUtilStat {
			if s.Rcompleted == 0 && s.Wcompleted == 0 {
				continue // skip devices which never doing IOs
			}
			for _, v := range diskstatsValueNames() {
				var desc = "node_diskstats_" + v
				ch <- prometheus.MustNewConstMetric(e.AllDesc[desc], prometheus.CounterValue, s.SingleStat(v), s.Device)
				cnt++
			}
		}
	}
	return cnt
}

// collectNetdevMetrics collects network interfaces usage metrics
func (e *prometheusExporter) collectNetdevMetrics(ch chan<- prometheus.Metric) (cnt int) {
	var netdevUtil stat.Netdevs
	ifsCnt, err := stat.CountLinesLocal(stat.ProcNetdev)
	if err == nil {
		netdevUtil = make(stat.Netdevs, ifsCnt)
		err := netdevUtil.ReadLocal()
		if err != nil {
			log.Errorln("failed to collect netdev metrics: ", err)
			return 0
		}

		for _, s := range netdevUtil {
			if s.Rpackets == 0 && s.Tpackets == 0 {
				continue // skip interfaces which never seen packets
			}

			for _, v := range netdevValueNames() {
				var desc = "node_netdev_" + v

				if (desc == "node_netdev_speed" || desc == "node_netdev_duplex") && s.Speed > 0 {
					ch <- prometheus.MustNewConstMetric(e.AllDesc[desc], prometheus.CounterValue, s.SingleStat(v), s.Ifname)
					cnt++
					continue
				}

				ch <- prometheus.MustNewConstMetric(e.AllDesc[desc], prometheus.CounterValue, s.SingleStat(v), s.Ifname)
				cnt++
			}
		}
	}
	return cnt
}

// collectFsMetrics collects mounted filesystems' usage metrics
func (e *prometheusExporter) collectFsMetrics(ch chan<- prometheus.Metric) (cnt int) {
	var fsStats = make(stat.FsStats, 0, 10)
	err := fsStats.ReadLocal()
	if err != nil {
		log.Errorln("failed to collect filesystem metrics: ", err)
		return 0
	}

	for _, fs := range fsStats {
		for _, usage := range []string{"total_bytes", "free_bytes", "available_bytes", "used_bytes", "reserved_bytes", "reserved_pct"} {
			// TODO: добавить fstype
			ch <- prometheus.MustNewConstMetric(e.AllDesc["node_filesystem_bytes"], prometheus.CounterValue, float64(fs.SingleStat(usage)), usage, fs.Device, fs.Mountpoint, fs.Mountflags)
			cnt++
		}
		for _, usage := range []string{"total_inodes", "free_inodes", "used_inodes"} {
			ch <- prometheus.MustNewConstMetric(e.AllDesc["node_filesystem_inodes"], prometheus.CounterValue, float64(fs.SingleStat(usage)), usage, fs.Device, fs.Mountpoint, fs.Mountflags)
			cnt++
		}
	}
	return cnt
}

// collectSysctlMetrics collects sysctl metrics
func (e *prometheusExporter) collectSysctlMetrics(ch chan<- prometheus.Metric) (cnt int) {
	for _, sysctl := range sysctlList() {
		value, err := stat.GetSysctl(sysctl)
		if err != nil {
			log.Errorln("failed to obtain sysctl: ", err)
			continue
		}
		ch <- prometheus.MustNewConstMetric(e.AllDesc["node_settings_sysctl"], prometheus.CounterValue, float64(value), sysctl)
		cnt++
	}
	return cnt
}

// collectCPUCoresState collects CPU cores operational states' metrics
func (e *prometheusExporter) collectCPUCoresState(ch chan<- prometheus.Metric) (cnt int) {
	// Collect total number of CPU cores
	online, offline, err := stat.CountCPU()
	if err != nil {
		log.Errorln("failed counting CPUs: ", err)
		return 0
	}
	total := online + offline
	for state, v := range map[string]int{"all": total, "online": online, "offline": offline} {
		ch <- prometheus.MustNewConstMetric(e.AllDesc["node_hardware_cores_total"], prometheus.CounterValue, float64(v), state)
		cnt++
	}
	return cnt
}

// collectCPUScalingGovernors collects metrics about CPUs scaling governors
func (e *prometheusExporter) collectCPUScalingGovernors(ch chan<- prometheus.Metric) (cnt int) {
	sg, err := stat.CountScalingGovernors()
	if err != nil {
		log.Errorln("failed counting scaling governors: ", err)
		return 0
	}
	if len(sg) > 0 {
		for k, v := range sg {
			ch <- prometheus.MustNewConstMetric(e.AllDesc["node_hardware_scaling_governors_total"], prometheus.CounterValue, float64(v), k)
			cnt++
		}
	} else {
		ch <- prometheus.MustNewConstMetric(e.AllDesc["node_hardware_scaling_governors_total"], prometheus.CounterValue, 0, "disabled")
		cnt++
	}
	return cnt
}

// collectNumaNodes collect metrics about configured NUMA nodes
func (e *prometheusExporter) collectNumaNodes(ch chan<- prometheus.Metric) (cnt int) {
	numa, err := stat.CountNumaNodes()
	if err != nil {
		log.Errorln("failed counting NUMA nodes: ", err)
		return 0
	}
	ch <- prometheus.MustNewConstMetric(e.AllDesc["node_hardware_numa_nodes"], prometheus.CounterValue, float64(numa))
	cnt++
	return cnt
}

// collectStorageSchedulers collect metrics about attached block devices, such as HDD, SSD, NVMe, etc.
func (e *prometheusExporter) collectStorageSchedulers(ch chan<- prometheus.Metric) (cnt int) {
	dirs, err := filepath.Glob("/sys/block/*")
	if err != nil {
		log.Warnln("skip collecting io schedulers: ", err)
		return 0
	}

	var devname, scheduler string
	var rotational float64
	for _, devpath := range dirs {
		re := regexp.MustCompile(regexpBlockDevicesExtended)

		if re.MatchString(devpath) {
			devname = strings.Replace(devpath, "/sys/block/", "/dev/", 1)
			rotational, err = stat.IsDeviceRotational(devpath)
			if err != nil {
				log.Warnf("skip collecting io schedulers for %s: %s", devname, err)
				continue
			}
			scheduler, err = stat.GetDeviceScheduler(devpath)
			if err != nil {
				log.Warnf("skip collecting io schedulers for %s: %s", devname, err)
				continue
			}
			ch <- prometheus.MustNewConstMetric(e.AllDesc["node_hardware_storage_rotational"], prometheus.GaugeValue, rotational, devname, scheduler)
			cnt++
		}
	}
	return cnt
}

// collectSystemUptime collects metric about system uptime
func (e *prometheusExporter) collectSystemUptime(ch chan<- prometheus.Metric) (cnt int) {
	uptime, err := stat.Uptime()
	if err != nil {
		log.Warnln("skip collecting system uptime: ", err)
		return 0
	}
	ch <- prometheus.MustNewConstMetric(e.AllDesc["node_uptime_seconds"], prometheus.CounterValue, uptime)
	return 1
}
