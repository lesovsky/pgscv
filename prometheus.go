//
package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/statgears/pgscv/stat"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

// структура-обертка для хранения всех метрик
type Exporter struct {
	// делает экспортер уникальным для конкретного сервиса на запущщеном хосте -- т.е. для N сервисов будет N экспортеров
	ServiceID string
	AllDesc   map[string]*prometheus.Desc
}

// структура содержит значение и набор меток, структура будет являться частью мапы которая определяет набор значений конкретной метрики, например
// pg_stat_database_xact_commits: { 4351, {1, pgbench}}, { 5241, {2, appdb}}, { 4918, {3, testdb}}, { 9812, {4, maindb}}
// структура хранит только одно значение для будущей метрики + значения меток для этой метрики.
// далее датумы для одной метрики но с разными значениями будут сгруппированы в рамках структуры MetricData
type RawMetricDatum struct {
	Value       string
	LabelValues []string
}

// хранилище конкретной метрики, содержит идентификтаор, описание для прометеуса и хэш-мапу всех значений для этой метрики
type MetricData struct { // эту структуру должна возвращать конкретная функция которая берет откуда-то стату и оформляет в вид подходящий для экспорта в прометеус
	MetricDesc *prometheus.Desc       // идентификатор метрики для прометеуса
	MetricType prometheus.ValueType   // тип этой метрики
	RawDataMap map[int]RawMetricDatum // набор значений метрики и значений всех ее меток
}

// источник статистики - имя, запрос, список полей-значений и полей-меток
type StatDesc struct {
	Name       string                          // имя источника откуда берется стата, выбирается произвольно и может быть как именем вьюхи, таблицы, функции, так и каким-то придуманным
	Stype      int                             // тип источника статы - постгрес, баунсер, система и т.п.
	Private    bool                            // является ли стата личной для конкретной базы? например стата для таблиц/индексов/функций -- применимо только к постгресовой стате
	Query      string                          // запрос с помощью которого вытягивается стата из источника
	ValueNames []string                        // названия полей которые будут использованы как значения метрик
	ValueTypes map[string]prometheus.ValueType //теоретически мапа нужна для хренения карты метрика <-> тип, например xact_commit <-> Counter/Gauge. Но пока поле не используется никак
	LabelNames []string                        // названия полей которые будут использованы как метки
}

const (
	STYPE_POSTGRESQL = iota
	STYPE_PGBOUNCER
	STYPE_SYSTEM

	// признак того какую стату следует собрать
	STAT_SHARED = iota
	STAT_PRIVATE
	STAT_ALL
)

//
var (
	diskstatsValueNames             = []string{"rcompleted", "rmerged", "rsectors", "rspent", "wcompleted", "wmerged", "wsectors", "wspent", "ioinprogress", "tspent", "tweighted", "uptime"}
	netdevValueNames                = []string{"rbytes", "rpackets", "rerrs", "rdrop", "rfifo", "rframe", "rcompressed", "rmulticast", "tbytes", "tpackets", "terrs", "tdrop", "tfifo", "tcolls", "tcarrier", "tcompressed", "saturation", "uptime", "speed", "duplex"}
	pgStatDatabasesValueNames       = []string{"xact_commit", "xact_rollback", "blks_read", "blks_hit", "tup_returned", "tup_fetched", "tup_inserted", "tup_updated", "tup_deleted", "conflicts", "temp_files", "temp_bytes", "deadlocks", "blk_read_time", "blk_write_time", "db_size"}
	pgStatUserTablesValueNames      = []string{"seq_scan", "seq_tup_read", "idx_scan", "idx_tup_fetch", "n_tup_ins", "n_tup_upd", "n_tup_del", "n_tup_hot_upd", "n_live_tup", "n_dead_tup", "n_mod_since_analyze", "vacuum_count", "autovacuum_count", "analyze_count", "autoanalyze_count"}
	pgStatioUserTablesValueNames    = []string{"heap_blks_read", "heap_blks_hit", "idx_blks_read", "idx_blks_hit", "toast_blks_read", "toast_blks_hit", "tidx_blks_read", "tidx_blks_hit"}
	pgStatUserIndexesValueNames     = []string{"idx_scan", "idx_tup_read", "idx_tup_fetch"}
	pgStatioUserIndexesValueNames   = []string{"idx_blks_read", "idx_blks_hit"}
	pgStatBgwriterValueNames        = []string{"checkpoints_timed", "checkpoints_req", "checkpoint_write_time", "checkpoint_sync_time", "buffers_checkpoint", "buffers_clean", "maxwritten_clean", "buffers_backend", "buffers_backend_fsync", "buffers_alloc"}
	pgStatUserFunctionsValueNames   = []string{"calls", "total_time", "self_time"}
	pgStatActivityValueNames        = []string{"conn_total", "conn_idle_total", "conn_idle_xact_total", "conn_active_total", "conn_waiting_total", "conn_others_total", "conn_prepared_total", "xact_max_duration"}
	pgStatActivityAutovacValueNames = []string{"workers_total", "antiwraparound_workers_total", "user_vacuum_total", "max_duration"}
	pgStatStatementsValueNames      = []string{"calls", "total_time", "rows", "shared_blks_hit", "shared_blks_read", "shared_blks_dirtied", "shared_blks_written", "local_blks_hit", "local_blks_read", "local_blks_dirtied", "local_blks_written", "temp_blks_read", "temp_blks_written", "blk_read_time", "blk_write_time"}
	pgStatReplicationValueNames     = []string{"pg_wal_bytes", "pending_lag_bytes", "write_lag_bytes", "flush_lag_bytes", "replay_lag_bytes", "total_lag_bytes", "write_lag_sec", "flush_lag_sec", "replay_lag_sec"}
	pgStatCurrentTempFilesVN        = []string{"files_total", "bytes_total", "oldest_file_age_seconds_max"}
	pgbouncerPoolsVN                = []string{"cl_active", "cl_waiting", "sv_active", "sv_idle", "sv_used", "sv_tested", "sv_login", "maxwait", "maxwait_us"}
	pgbouncerStatsVN                = []string{"xact_count", "query_count", "bytes_received", "bytes_sent", "xact_time", "query_time", "wait_time"}

	sysctlList = []string{"kernel.sched_migration_cost_ns", "kernel.sched_autogroup_enabled",
		"vm.dirty_background_bytes", "vm.dirty_bytes", "vm.overcommit_memory", "vm.overcommit_ratio", "vm.swappiness", "vm.min_free_kbytes",
		"vm.zone_reclaim_mode", "kernel.numa_balancing", "vm.nr_hugepages", "vm.nr_overcommit_hugepages"}

	statdesc = []*StatDesc{
		{Name: "pg_stat_database", Query: pgStatDatabaseQuery, ValueNames: pgStatDatabasesValueNames, LabelNames: []string{"datid", "datname"}},
		{Name: "pg_stat_user_tables", Query: pgStatUserTablesQuery, Private: true, ValueNames: pgStatUserTablesValueNames, LabelNames: []string{"datname", "schemaname", "relname"}},
		{Name: "pg_statio_user_tables", Query: pgStatioUserTablesQuery, Private: true, ValueNames: pgStatioUserTablesValueNames, LabelNames: []string{"datname", "schemaname", "relname"}},
		{Name: "pg_stat_user_indexes", Query: pgStatUserIndexesQuery, Private: true, ValueNames: pgStatUserIndexesValueNames, LabelNames: []string{"datname", "schemaname", "relname", "indexrelname"}},
		{Name: "pg_statio_user_indexes", Query: pgStatioUserIndexesQuery, Private: true, ValueNames: pgStatioUserIndexesValueNames, LabelNames: []string{"datname", "schemaname", "relname", "indexrelname"}},
		{Name: "pg_stat_bgwriter", Query: pgStatBgwriterQuery, ValueNames: pgStatBgwriterValueNames, LabelNames: []string{}},
		{Name: "pg_stat_user_functions", Query: pgStatUserFunctionsQuery, Private: true, ValueNames: pgStatUserFunctionsValueNames, LabelNames: []string{"funcid", "datname", "schemaname", "funcname"}},
		{Name: "pg_stat_activity", Query: pgStatActivityQuery, ValueNames: pgStatActivityValueNames, LabelNames: []string{}},
		{Name: "pg_stat_activity_autovac", Query: pgStatActivityAutovacQuery, ValueNames: pgStatActivityAutovacValueNames, LabelNames: []string{}},
		{Name: "pg_stat_statements", Query: pgStatStatementsQuery, ValueNames: pgStatStatementsValueNames, LabelNames: []string{"usename", "datname", "queryid", "query"}},
		{Name: "pg_stat_replication", Query: pgStatReplicationQuery, ValueNames: pgStatReplicationValueNames, LabelNames: []string{"client_addr", "application_name"}},
		{Name: "pg_replication_slots", Query: pgReplicationSlotsQuery, ValueNames: []string{"restart_lag_bytes"}, LabelNames: []string{"slot_name", "active"}},
		{Name: "pg_stat_basebackup", Query: pgStatBasebackupQuery, ValueNames: []string{"count", "duration_seconds_max"}, LabelNames: []string{}},
		{Name: "pg_stat_current_temp", Query: pgStatCurrentTempFilesQuery, ValueNames: pgStatCurrentTempFilesVN, LabelNames: []string{"tablespace"}},
		{Name: "pg_wal_directory", Query: pgStatWalSizeQuery, ValueNames: []string{"size_bytes"}, LabelNames: []string{}},
		{Name: "pg_data_directory", Query: "", Private: false, LabelNames: []string{"device", "mountpoint"}},
		{Name: "pg_settings", Query: pgSettingsGucQuery, ValueNames: []string{"guc"}, LabelNames: []string{"name", "unit", "secondary"}},
		// system metrics
		{Name: "node_cpu_usage", Stype: STYPE_SYSTEM, ValueNames: []string{"time"}, LabelNames: []string{"mode"}},
		{Name: "node_diskstats", Stype: STYPE_SYSTEM, ValueNames: diskstatsValueNames, LabelNames: []string{"device"}},
		{Name: "node_netdev", Stype: STYPE_SYSTEM, ValueNames: netdevValueNames, LabelNames: []string{"interface"}},
		{Name: "node_memory", Stype: STYPE_SYSTEM, ValueNames: []string{"usage_bytes"}, LabelNames: []string{"usage"}},
		{Name: "node_filesystem", Stype: STYPE_SYSTEM, ValueNames: []string{"bytes", "inodes"}, LabelNames: []string{"usage", "device", "mountpoint", "flags"}},
		{Name: "node_settings", Stype: STYPE_SYSTEM, ValueNames: []string{"sysctl"}, LabelNames: []string{"sysctl"}},
		{Name: "node_hardware_cores", Stype: STYPE_SYSTEM, ValueNames: []string{"total"}, LabelNames: []string{"state"}},
		{Name: "node_hardware_numa", Stype: STYPE_SYSTEM, ValueNames: []string{"nodes"}},
		{Name: "node_hardware_storage_rotational", Stype: STYPE_SYSTEM, LabelNames: []string{"device", "scheduler"}},
		// pgbouncer metrics
		{Name: "pgbouncer_pool", Stype: STYPE_PGBOUNCER, Query: "SHOW POOLS", ValueNames: pgbouncerPoolsVN, LabelNames: []string{"database", "user", "pool_mode"}},
		{Name: "pgbouncer_stats", Stype: STYPE_PGBOUNCER, Query: "SHOW STATS_TOTALS", ValueNames: pgbouncerStatsVN, LabelNames: []string{"database"}},
	}
)

// TODO: pull режим не отдает системные метрики

//
func adjustQueries(descs []*StatDesc, pgVersion int) {
	for _, desc := range descs {
		switch desc.Name {
		case "pg_stat_replication":
			switch {
			case pgVersion < 100000:
				desc.Query = pgStatReplicationQuery96
			}
		case "pg_replication_slots":
			switch {
			case pgVersion < 100000:
				desc.Query = pgReplicationSlotsQuery96
			}
		case "pg_wal_directory":
			switch {
			case pgVersion < 100000:
				desc.Query = pgStatWalSizeQuery96
			}
		}
	}
}

//
func NewExporter(itype int, cfid string, sid string) (*Exporter, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	var e = make(map[string]*prometheus.Desc)
	for _, desc := range statdesc {
		if itype == desc.Stype {
			if len(desc.ValueNames) > 0 {
				for _, suffix := range desc.ValueNames {
					var metric_name = desc.Name + "_" + suffix
					e[metric_name] = prometheus.NewDesc(metric_name, metricsHelp[metric_name], desc.LabelNames, prometheus.Labels{"cfid": cfid, "sid": sid, "db_instance": hostname})
				}
			} else {
				e[desc.Name] = prometheus.NewDesc(desc.Name, metricsHelp[desc.Name], desc.LabelNames, prometheus.Labels{"cfid": cfid, "sid": sid, "db_instance": hostname})
			}

		}
	}

	return &Exporter{ServiceID: sid, AllDesc: e}, nil
}

//
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range e.AllDesc {
		ch <- desc
	}
}

//
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	var metricsCnt int

	for i := range Instances {
		if e.ServiceID == Instances[i].ServiceId {
			log.Debugf("%s: start collecting metrics\n", time.Now().Format("2006-01-02 15:04:05"))

			// в зависимости от типа экспортера делаем соотв.проверки
			switch Instances[i].InstanceType {
			case STYPE_POSTGRESQL, STYPE_PGBOUNCER:
				metricsCnt += e.collectPgMetrics(ch, Instances[i])
			case STYPE_SYSTEM:
				metricsCnt += e.collectCpuMetrics(ch)
				metricsCnt += e.collectMemMetrics(ch)
				metricsCnt += e.collectDiskstatsMetrics(ch)
				metricsCnt += e.collectNetdevMetrics(ch)
				metricsCnt += e.collectFsMetrics(ch)
				metricsCnt += e.collectSysctlMetrics(ch)
				metricsCnt += e.collectHardwareMetrics(ch)
			}
		}
	}

	log.Debugf("%s: generated %d metrics\n", time.Now().Format("2006-01-02 15:04:05"), metricsCnt)
}

//
func (e *Exporter) collectCpuMetrics(ch chan<- prometheus.Metric) (cnt int) {
	var cpuStat stat.CpuRawstat
	cpuStat.ReadLocal()
	for _, mode := range []string{"user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal", "guest", "guest_nice", "total"} {
		ch <- prometheus.MustNewConstMetric(e.AllDesc["node_cpu_usage_time"], prometheus.CounterValue, cpuStat.SingleStat(mode), mode)
		cnt += 1
	}
	return cnt
}

//
func (e *Exporter) collectMemMetrics(ch chan<- prometheus.Metric) (cnt int) {
	var meminfoStat stat.Meminfo
	meminfoStat.ReadLocal()
	for _, usage := range []string{"mem_total", "mem_free", "mem_used", "swap_total", "swap_free", "swap_used", "mem_cached", "mem_dirty", "mem_writeback", "mem_buffers", "mem_available", "mem_slab"} {
		ch <- prometheus.MustNewConstMetric(e.AllDesc["node_memory_usage_bytes"], prometheus.GaugeValue, float64(meminfoStat.SingleStat(usage)), usage)
		cnt += 1
	}
	return cnt
}

//
func (e *Exporter) collectDiskstatsMetrics(ch chan<- prometheus.Metric) (cnt int) {
	var diskUtilStat stat.Diskstats
	bdev_cnt, err := stat.CountLinesLocal(stat.PROC_DISKSTATS)
	if err == nil {
		diskUtilStat = make(stat.Diskstats, bdev_cnt)
		diskUtilStat.ReadLocal() // TODO: errcheck, see collectHardwareMetrics() example
		for _, s := range diskUtilStat {
			if s.Rcompleted == 0 && s.Wcompleted == 0 {
				continue // skip devices which never doing IOs
			}
			for _, v := range diskstatsValueNames {
				var desc string = "node_diskstats_" + v
				ch <- prometheus.MustNewConstMetric(e.AllDesc[desc], prometheus.CounterValue, s.SingleStat(v), s.Device)
				cnt += 1
			}
		}
	}
	return cnt
}

//
func (e *Exporter) collectNetdevMetrics(ch chan<- prometheus.Metric) (cnt int) {
	var netdevUtil stat.Netdevs
	ifs_cnt, err := stat.CountLinesLocal(stat.PROC_NETDEV)
	if err == nil {
		netdevUtil = make(stat.Netdevs, ifs_cnt)
		netdevUtil.ReadLocal() // TODO: errcheck, see collectHardwareMetrics() example
		for _, s := range netdevUtil {
			if s.Rpackets == 0 && s.Tpackets == 0 {
				continue // skip interfaces which never seen packets
			}

			for _, v := range netdevValueNames {
				var desc string = "node_netdev_" + v

				// TODO: вроде эти метрики не нужны
				if (desc == "speed" || desc == "duplex") && s.Speed > 0 {
					ch <- prometheus.MustNewConstMetric(e.AllDesc[desc], prometheus.GaugeValue, s.SingleStat(v), s.Ifname)
					cnt += 1
					continue
				}

				ch <- prometheus.MustNewConstMetric(e.AllDesc[desc], prometheus.CounterValue, s.SingleStat(v), s.Ifname)
				cnt += 1
			}
		}
	}
	return cnt
}

// Collects metrics about mounted filesystems
func (e *Exporter) collectFsMetrics(ch chan<- prometheus.Metric) (cnt int) {
	var fsStats = make(stat.FsStats, 0, 10)
	fsStats.ReadLocal() // TODO: errcheck, see collectHardwareMetrics() example
	for _, fs := range fsStats {
		for _, usage := range []string{"total_bytes", "free_bytes", "available_bytes", "used_bytes", "reserved_bytes", "reserved_pct"} {
			// TODO: добавить fstype
			ch <- prometheus.MustNewConstMetric(e.AllDesc["node_filesystem_bytes"], prometheus.CounterValue, float64(fs.SingleStat(usage)), usage, fs.Device, fs.Mountpoint, fs.Mountflags)
			cnt += 1
		}
		for _, usage := range []string{"total_inodes", "free_inodes", "used_inodes"} {
			ch <- prometheus.MustNewConstMetric(e.AllDesc["node_filesystem_inodes"], prometheus.CounterValue, float64(fs.SingleStat(usage)), usage, fs.Device, fs.Mountpoint, fs.Mountflags)
			cnt += 1
		}
	}

	return cnt
}

// Read sysctl variables and translate them to metrics
func (e *Exporter) collectSysctlMetrics(ch chan<- prometheus.Metric) (cnt int) {
	for _, sysctl := range sysctlList {
		value, err := stat.GetSysctl(sysctl)
		if err != nil {
			log.Errorf("failed to obtain sysctl: err", err)
			continue
		}
		ch <- prometheus.MustNewConstMetric(e.AllDesc["node_settings_sysctl"], prometheus.CounterValue, float64(value), sysctl)
		cnt += 1
	}

	return cnt
}

// Collects metrics about running system - hardware configuration
func (e *Exporter) collectHardwareMetrics(ch chan<- prometheus.Metric) (cnt int) {
	// Collect total number of CPU cores
	online, offline, err := stat.CountCpu()
	if err != nil {
		log.Errorf("failed counting CPUs: err", err)
		return 0
	}
	total := online + offline
	for state, v := range map[string]int{"all": total, "online": online, "offline": offline} {
		ch <- prometheus.MustNewConstMetric(e.AllDesc["node_hardware_cores_total"], prometheus.CounterValue, float64(v), state)
		cnt++
	}

	// Collect total number of NUMA nodes
	numa, err := stat.CountNumaNodes()
	if err != nil {
		log.Errorf("failed counting NUMA nodes: err", err)
		return cnt
	}
	ch <- prometheus.MustNewConstMetric(e.AllDesc["node_hardware_numa_nodes"], prometheus.CounterValue, float64(numa))
	cnt++

	// Collect info about storage (attached HDD, SSD, NVMe, etc.
	cnt += getStorageInfo(e, ch)
	return cnt

}

//
func getStorageInfo(e *Exporter, ch chan<- prometheus.Metric) (cnt int) {
	dirs, err := filepath.Glob("/sys/block/*")
	if err != nil {
		fmt.Println(err)
	}

	var devname, scheduler string
	var rotational float64
	for _, devpath := range dirs {
		re := regexp.MustCompile(`((s|xv|v)d[a-z])|(nvme[0-9]n[0-9])|(dm-[0-9]+)|(md[0-9]+)`)

		if re.MatchString(devpath) {
			devname = strings.Replace(devpath, "/sys/block/", "/dev/", 1)
			rotational, err = stat.IsDeviceRotational(devpath)
			if err != nil {
				log.Warnln(err)
				continue
			}
			scheduler, err = stat.GetDeviceScheduler(devpath)
			if err != nil {
				log.Warnln(err)
				continue
			}
			ch <- prometheus.MustNewConstMetric(e.AllDesc["node_hardware_storage_rotational"], prometheus.GaugeValue, rotational, devname, scheduler)
			cnt++
		}
	}
	return cnt
}

// Собираем стату постгреса или баунсера.
// Сначала определяем тип экспортера и соотв. какую стату хотим собрать -- баунсерную или постгресовую.
// В случае постгресовой статы, в начале подключаемся к постгресу и собираем список баз (далее мы будем подключаться к этим базам и собирать стату по объектам базы)
// В любом случае формируем т.н. список баз, в самом простом случае там будет как минимум 1 имя базы - дефолтное имя обнаржуенное при авто-дискавери
// После того как список сформирован, создаем хранилище для собранных данных куда будет складываться все данные собранные от постгреса или баунсера. На основе данных из этого хранилища будем генерить метрики для прометеуса
// Начинаем с того что проходимся в цикле по списку баз, устанавливаем соединение с этой базой, смотрим ее версию, адаптируем запросы под конкретную версию и запускаем сбор статы.
// При первой итерации сбора статы всегда собираем всю стату - и шаредную и приватную. После сбора закрываем соединение.
// После того как стата собрана, на основе данных хранилища формируем метрики для прометеуса. Учитывая что шаредная стата уже собрана, в последующих циклам собираем только приватную стату. И так пока на дойдем до конца списка баз
func (e *Exporter) collectPgMetrics(ch chan<- prometheus.Metric, instance Instance) (cnt int) {
	var dblist []string

	// формируем список баз -- как минимум в этот список будет входить база из автодискавери
	// TODO: тут можно облажаться со сбором pg_stat_statements, например в первую итерацию попадает база без модуля, а в последующих циклах со снятым флагом, стату по pg_stat_statements уже не будет собираться
	// также имеет смысл чекать наличие pgq схемы
	if instance.InstanceType == STYPE_POSTGRESQL {
		conn, err := CreateConn(&instance)
		if err != nil {
			log.Warnf("Failed to connect: %s, skip", err.Error())
			return 0
		}
		if err := PQstatus(conn, instance.InstanceType); err != nil {
			log.Warnf("Failed to check status: %s, skip", err.Error())
			remove_instance <- instance.Pid // удаляем инстанс их хэш карты
			return 0
		}

		dblist, err = getDBList(conn)
		if err != nil {
			log.Warnf("Failed to get list of databases: %s. Use default database name: %s", err, instance.Dbname)
			dblist = []string{instance.Dbname}
		}

		conn.Close()
	} else {
		dblist = []string{"pgbouncer"}
	}

	// теперь нужно пройтись по всем базам и собрать стату
	var target = STAT_ALL // при первой попытке сбора пытаемся собрать всю имеющуюся стату

	for _, dbname := range dblist {
		instance.Dbname = dbname

		conn, err := CreateConn(&instance) // открываем коннект к базе
		if err != nil {
			log.Warnf("Failed to connect: %s, skip", err.Error())
			return 0
		}

		// адаптируем запросы под конкретную версию
		if target == STAT_ALL && instance.InstanceType == STYPE_POSTGRESQL {
			var version int
			if err := conn.QueryRow(pgVersionNumQuery).Scan(&version); err != nil {
				log.Warnf("Failed to obtain PostgreSQL version: %s. Skipping stats collecting for %s database", err, dbname)
				continue
			}
			adjustQueries(statdesc, version)
		}

		// собираем стату
		e.getPgStat(conn, ch, instance.InstanceType, target)
		conn.Close() // закрываем соединение

		target = STAT_PRIVATE // как только шаредная стата собрана, не имеет смысла ее собирать еще раз, далее собираем только приватную стату.
	}

	return cnt
}

// задача функции собрать стату в зависимости от потребности - шаредную или приватную.
// Шаредная стата описывает кластер целиком, приватная относится к конкретной базе и описывает таблицы/индексы/функции которые принадлежат этой базе
// Для сбора статы обходим все имеющиеся источники и пропускаем ненужные. Далее выполняем запрос ассоциированный с источником и делаем его в подключение.
// Полученный ответ от базы оформляем в массив данных и складываем в общее хранилище в котором собраны данные от всех ответов, когда все источники обшарены возвращаем наружу общее хранилище с собранными данными
func (e *Exporter) getPgStat(conn *sql.DB, ch chan<- prometheus.Metric, itype int, target int) {
	// обходим по всем источникам
	for _, desc := range statdesc {
		if desc.Stype == itype {
			switch target {
			case STAT_SHARED:
				if desc.Private {
					continue // нам нужно собрать шаредную стату, соотв. пропускаем всю приватную
				}
			case STAT_PRIVATE:
				if !desc.Private {
					continue // нам нужно собрать приватную стату, соотв. пропускаем всю шаредную
				}
			case STAT_ALL:
				// ничего не пропускаем, т.к. надо собрать и приватную и шаредную статы
			}

			if desc.Query == "" {
				getDatadirInfo(e, conn, ch)
			}

			rows, err := conn.Query(desc.Query)
			// Errors aren't critical for us, remember and show them to the user. Return after the error, because
			// there is no reason to continue.
			if err != nil {
				log.Warnf("Failed to execute query: %s\n%s", err, desc.Query)
				continue // если произошла ошибка, то пропускаем этот конкретный шаг сбора статы
			}

			var container []sql.NullString
			var pointers []interface{}

			colnames, _ := rows.Columns()
			ncols := len(colnames)

			for rows.Next() {
				pointers = make([]interface{}, ncols)
				container = make([]sql.NullString, ncols)

				for i := range pointers {
					pointers[i] = &container[i]
				}

				err := rows.Scan(pointers...)
				if err != nil {
					log.Warnf("Failed to scan query result: %s\n%s", err, desc.Query)
					continue // если произошла ошибка, то пропускаем эту строку и переходим к следующей
				}

				for c, colname := range colnames {
					// Если колонки нет в списке меток, то генерим метрику на основе значения [row][column]. Если имя колонки входит в список меток, то пропускаем ее -- нам не нужно генерить из нее метрику, т.к. она как метка+значение сама будет частью метрики
					if !Contains(desc.LabelNames, colname) {
						var labelValues = make([]string, len(desc.LabelNames))
						// итерируемся по именам меток, нужно собрать из результата-ответа от базы, значения для соотв. меток
						for i, lname := range desc.LabelNames {
							// определяем номер (индекс) колонки в PGresult, который соотв. названию метки -- по этому индексу возьмем значение для метки из PGresult (таким образом мы не привязываемся к порядку полей в запросе)
							for idx, cname := range colnames {
								if cname == lname {
									labelValues[i] = container[idx].String
								}
							}
						}

						var metricValue string = container[c].String
						v, err := strconv.ParseFloat(metricValue, 64) // преобразуем string в подходящий для прометеуса float
						if err != nil {
							//log.Warnf("WARNING: can't convert to float: %s\n", err)	// TODO: включить варнинг и найти места где не получается распарсить во флоат
							continue
						}

						ch <- prometheus.MustNewConstMetric(
							e.AllDesc[desc.Name+"_"+colname], // *prometheus.Desc который также участвует в Describe методе
							prometheus.CounterValue,          // тип метрики
							v,                                // значение метрики
							labelValues...,                   // массив меток
						)
					}
				}
			}
			rows.Close()
		}
	}
}

// getDatadirInfo evaluates data_directory's mountpoint
func getDatadirInfo(e *Exporter, conn *sql.DB, ch chan<- prometheus.Metric) {
	var dataDir string
	if err := conn.QueryRow(`SELECT current_setting('data_directory')`).Scan(&dataDir); err != nil {
		return
	}

	mountpoints := stat.ReadMounts()
	dirpath := stat.RewritePath(dataDir)

	parts := strings.Split(dirpath, "/")
	for i := len(parts); i > 0; i-- {
		if subpath := strings.Join(parts[0:i], "/"); subpath != "" {
			// check is subpath a symlink? if symlink - dereference and replace it
			fi, _ := os.Lstat(subpath)
			if fi.Mode() & os.ModeSymlink != 0 {
				resolvedLink, err := os.Readlink(subpath)
				if err != nil {
					log.Warnf("failed to resolve symlink %s: %s\n", subpath, err)
					return
				}

				if _, ok := mountpoints[resolvedLink]; ok {
					subpath = resolvedLink
				}
			}
			if device, ok := mountpoints[subpath]; ok {
				ch <- prometheus.MustNewConstMetric(e.AllDesc["pg_data_directory"], prometheus.GaugeValue, 1, device, subpath)
				return
			}
		} else {
			device := mountpoints["/"]
			ch <- prometheus.MustNewConstMetric(e.AllDesc["pg_data_directory"], prometheus.GaugeValue, 1, device, "/")
			return
		}
	}
}
