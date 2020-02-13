//
package app

import (
	"database/sql"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"path/filepath"
	"pgscv/app/model"
	"pgscv/app/stat"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// PrometheusExporter is the realization of prometheus.Collector
type PrometheusExporter struct {
	Logger      zerolog.Logger              // logging
	ServiceID   string                      // unique ID across all services
	AllDesc     map[string]*prometheus.Desc // metrics assigned to this exporter
	ServiceRepo ServiceRepo                 // service repository
	TotalFailed int                         // total number of collecting failures
}

// StatDesc is the statistics descriptor, with detailed info about particular kind of stats
type StatDesc struct {
	Name           string                          // имя источника откуда берется стата, выбирается произвольно и может быть как именем вьюхи, таблицы, функции, так и каким-то придуманным
	Stype          int                             // тип источника статы - постгрес, баунсер, система и т.п.
	Query          string                          // запрос с помощью которого вытягивается стата из источника
	ValueNames     []string                        // названия полей которые будут использованы как значения метрик
	ValueTypes     map[string]prometheus.ValueType //теоретически мапа нужна для хренения карты метрика <-> тип, например xact_commit <-> Counter/Gauge. Но пока поле не используется никак
	LabelNames     []string                        // названия полей которые будут использованы как метки
	collectDone    bool                            // стата уже собрана (для всяких шаредных стат типа pg_stat_bgwriter, pg_stat_database)
	collectOneshot bool                            // стату собирать только один раз за раунд, (например всякие шаредные статы тип pg_stat_database)
	Schedule                                       // расписание по которому осуществляется сбор метрик
}

const (
	// regexp describes raw block devices except their partitions, but including stacked devices, such as device-mapper and mdraid
	regexpBlockDevicesExtended = `((s|xv|v)d[a-z])|(nvme[0-9]n[0-9])|(dm-[0-9]+)|(md[0-9]+)`

	// how many failures should occur before unregistering exporter
	exporterFailureLimit int = 10
)

var (
	diskstatsValueNames               = []string{"rcompleted", "rmerged", "rsectors", "rspent", "wcompleted", "wmerged", "wsectors", "wspent", "ioinprogress", "tspent", "tweighted", "uptime"}
	netdevValueNames                  = []string{"rbytes", "rpackets", "rerrs", "rdrop", "rfifo", "rframe", "rcompressed", "rmulticast", "tbytes", "tpackets", "terrs", "tdrop", "tfifo", "tcolls", "tcarrier", "tcompressed", "saturation", "uptime", "speed", "duplex"}
	pgStatDatabasesValueNames         = []string{"xact_commit", "xact_rollback", "blks_read", "blks_hit", "tup_returned", "tup_fetched", "tup_inserted", "tup_updated", "tup_deleted", "conflicts", "temp_files", "temp_bytes", "deadlocks", "blk_read_time", "blk_write_time", "db_size", "stats_age_seconds"}
	pgStatUserTablesValueNames        = []string{"seq_scan", "seq_tup_read", "idx_scan", "idx_tup_fetch", "n_tup_ins", "n_tup_upd", "n_tup_del", "n_tup_hot_upd", "n_live_tup", "n_dead_tup", "n_mod_since_analyze", "vacuum_count", "autovacuum_count", "analyze_count", "autoanalyze_count"}
	pgStatioUserTablesValueNames      = []string{"heap_blks_read", "heap_blks_hit", "idx_blks_read", "idx_blks_hit", "toast_blks_read", "toast_blks_hit", "tidx_blks_read", "tidx_blks_hit"}
	pgStatUserIndexesValueNames       = []string{"idx_scan", "idx_tup_read", "idx_tup_fetch"}
	pgStatioUserIndexesValueNames     = []string{"idx_blks_read", "idx_blks_hit"}
	pgStatBgwriterValueNames          = []string{"checkpoints_timed", "checkpoints_req", "checkpoint_write_time", "checkpoint_sync_time", "buffers_checkpoint", "buffers_clean", "maxwritten_clean", "buffers_backend", "buffers_backend_fsync", "buffers_alloc"}
	pgStatUserFunctionsValueNames     = []string{"calls", "total_time", "self_time"}
	pgStatActivityValueNames          = []string{"conn_total", "conn_idle_total", "conn_idle_xact_total", "conn_active_total", "conn_waiting_total", "conn_others_total", "conn_prepared_total"}
	pgStatActivityDurationsNames      = []string{"max_seconds", "idle_xact_max_seconds", "wait_max_seconds"}
	pgStatActivityAutovacValueNames   = []string{"workers_total", "antiwraparound_workers_total", "user_vacuum_total", "max_duration"}
	pgStatStatementsValueNames        = []string{"calls", "rows", "total_time", "blk_read_time", "blk_write_time", "shared_blks_hit", "shared_blks_read", "shared_blks_dirtied", "shared_blks_written", "local_blks_hit", "local_blks_read", "local_blks_dirtied", "local_blks_written", "temp_blks_read", "temp_blks_written"}
	pgStatReplicationValueNames       = []string{"pg_wal_bytes", "pending_lag_bytes", "write_lag_bytes", "flush_lag_bytes", "replay_lag_bytes", "total_lag_bytes", "write_lag_sec", "flush_lag_sec", "replay_lag_sec"}
	pgStatDatabaseConflictsValueNames = []string{"total", "tablespace", "lock", "snapshot", "bufferpin", "deadlock"}
	pgStatCurrentTempFilesVN          = []string{"files_total", "bytes_total", "oldest_file_age_seconds_max"}
	pgbouncerPoolsVN                  = []string{"cl_active", "cl_waiting", "sv_active", "sv_idle", "sv_used", "sv_tested", "sv_login", "maxwait", "maxwait_us"}
	pgbouncerStatsVN                  = []string{"xact_count", "query_count", "bytes_received", "bytes_sent", "xact_time", "query_time", "wait_time"}

	sysctlList = []string{"kernel.sched_migration_cost_ns", "kernel.sched_autogroup_enabled",
		"vm.dirty_background_bytes", "vm.dirty_bytes", "vm.overcommit_memory", "vm.overcommit_ratio", "vm.swappiness", "vm.min_free_kbytes",
		"vm.zone_reclaim_mode", "kernel.numa_balancing", "vm.nr_hugepages", "vm.nr_overcommit_hugepages"}

	statdesc = []*StatDesc{
		// collect oneshot -- these Postgres statistics are collected once per round
		{Name: "pg_stat_database", Stype: model.ServiceTypePostgresql, Query: pgStatDatabaseQuery, collectOneshot: true, ValueNames: pgStatDatabasesValueNames, LabelNames: []string{"datid", "datname"}},
		{Name: "pg_stat_bgwriter", Stype: model.ServiceTypePostgresql, Query: pgStatBgwriterQuery, collectOneshot: true, ValueNames: pgStatBgwriterValueNames, LabelNames: []string{}},
		{Name: "pg_stat_user_functions", Stype: model.ServiceTypePostgresql, Query: pgStatUserFunctionsQuery, ValueNames: pgStatUserFunctionsValueNames, LabelNames: []string{"funcid", "datname", "schemaname", "funcname"}},
		{Name: "pg_stat_activity", Stype: model.ServiceTypePostgresql, Query: pgStatActivityQuery, collectOneshot: true, ValueNames: pgStatActivityValueNames, LabelNames: []string{}},
		{Name: "pg_stat_activity", Stype: model.ServiceTypePostgresql, Query: pgStatActivityDurationsQuery, collectOneshot: true, ValueNames: pgStatActivityDurationsNames, LabelNames: []string{}},
		{Name: "pg_stat_activity_autovac", Stype: model.ServiceTypePostgresql, Query: pgStatActivityAutovacQuery, collectOneshot: true, ValueNames: pgStatActivityAutovacValueNames, LabelNames: []string{}},
		{Name: "pg_stat_statements", Stype: model.ServiceTypePostgresql, Query: pgStatStatementsQuery, collectOneshot: true, ValueNames: pgStatStatementsValueNames, LabelNames: []string{"usename", "datname", "queryid", "query"}},
		{Name: "pg_stat_replication", Stype: model.ServiceTypePostgresql, Query: pgStatReplicationQuery, collectOneshot: true, ValueNames: pgStatReplicationValueNames, LabelNames: []string{"client_addr", "application_name"}},
		{Name: "pg_replication_slots_restart_lag", Stype: model.ServiceTypePostgresql, Query: pgReplicationSlotsQuery, collectOneshot: true, ValueNames: []string{"bytes"}, LabelNames: []string{"slot_name", "active"}},
		{Name: "pg_replication_slots", Stype: model.ServiceTypePostgresql, Query: pgReplicationSlotsCountQuery, collectOneshot: true, ValueNames: []string{"conn"}, LabelNames: []string{"state"}},
		{Name: "pg_replication_standby", Stype: model.ServiceTypePostgresql, Query: pgReplicationStandbyCount, collectOneshot: true, ValueNames: []string{"count"}, LabelNames: []string{}},
		{Name: "pg_recovery", Stype: model.ServiceTypePostgresql, Query: pgRecoveryStatusQuery, collectOneshot: true, ValueNames: []string{"status"}},
		{Name: "pg_stat_database_conflicts", Stype: model.ServiceTypePostgresql, Query: pgStatDatabaseConflictsQuery, collectOneshot: true, ValueNames: pgStatDatabaseConflictsValueNames, LabelNames: []string{}},
		{Name: "pg_stat_basebackup", Stype: model.ServiceTypePostgresql, Query: pgStatBasebackupQuery, collectOneshot: true, ValueNames: []string{"count", "duration_seconds_max"}, LabelNames: []string{}},
		{Name: "pg_stat_current_temp", Stype: model.ServiceTypePostgresql, Query: pgStatCurrentTempFilesQuery, collectOneshot: true, ValueNames: pgStatCurrentTempFilesVN, LabelNames: []string{"tablespace"}},
		{Name: "pg_data_directory", Stype: model.ServiceTypePostgresql, Query: "", collectOneshot: true, LabelNames: []string{"device", "mountpoint", "path"}, Schedule: Schedule{Interval: 5 * time.Minute}},
		{Name: "pg_wal_directory", Stype: model.ServiceTypePostgresql, Query: "", collectOneshot: true, LabelNames: []string{"device", "mountpoint", "path"}, Schedule: Schedule{Interval: 5 * time.Minute}},
		{Name: "pg_log_directory", Stype: model.ServiceTypePostgresql, Query: "", collectOneshot: true, LabelNames: []string{"device", "mountpoint", "path"}, Schedule: Schedule{Interval: 5 * time.Minute}},
		{Name: "pg_wal_directory", Stype: model.ServiceTypePostgresql, Query: pgStatWalSizeQuery, collectOneshot: true, ValueNames: []string{"size_bytes"}, LabelNames: []string{}, Schedule: Schedule{Interval: 5 * time.Minute}},
		{Name: "pg_log_directory", Stype: model.ServiceTypePostgresql, Query: pgLogdirSizeQuery, collectOneshot: true, ValueNames: []string{"size_bytes"}, LabelNames: []string{}, Schedule: Schedule{Interval: 5 * time.Minute}},
		{Name: "pg_catalog_size", Stype: model.ServiceTypePostgresql, Query: pgCatalogSizeQuery, ValueNames: []string{"bytes"}, LabelNames: []string{"datname"}, Schedule: Schedule{Interval: 5 * time.Minute}},
		{Name: "pg_settings", Stype: model.ServiceTypePostgresql, Query: pgSettingsGucQuery, collectOneshot: true, ValueNames: []string{"guc"}, LabelNames: []string{"name", "unit", "secondary"}, Schedule: Schedule{Interval: 5 * time.Minute}},
		// collect always -- these Postgres statistics are collected every time in all databases
		{Name: "pg_stat_user_tables", Stype: model.ServiceTypePostgresql, Query: pgStatUserTablesQuery, ValueNames: pgStatUserTablesValueNames, LabelNames: []string{"datname", "schemaname", "relname"}},
		{Name: "pg_statio_user_tables", Stype: model.ServiceTypePostgresql, Query: pgStatioUserTablesQuery, ValueNames: pgStatioUserTablesValueNames, LabelNames: []string{"datname", "schemaname", "relname"}},
		{Name: "pg_stat_user_indexes", Stype: model.ServiceTypePostgresql, Query: pgStatUserIndexesQuery, ValueNames: pgStatUserIndexesValueNames, LabelNames: []string{"datname", "schemaname", "relname", "indexrelname"}},
		{Name: "pg_statio_user_indexes", Stype: model.ServiceTypePostgresql, Query: pgStatioUserIndexesQuery, ValueNames: pgStatioUserIndexesValueNames, LabelNames: []string{"datname", "schemaname", "relname", "indexrelname"}},
		{Name: "pg_schema_non_pk_table", Stype: model.ServiceTypePostgresql, Query: pgSchemaNonPrimaryKeyTablesQuery, ValueNames: []string{"exists"}, LabelNames: []string{"datname", "schemaname", "relname"}, Schedule: Schedule{Interval: 5 * time.Minute}},
		{Name: "pg_schema_invalid_index", Stype: model.ServiceTypePostgresql, Query: pgSchemaInvalidIndexesQuery, ValueNames: []string{"bytes"}, LabelNames: []string{"datname", "schemaname", "relname", "indexrelname"}, Schedule: Schedule{Interval: 5 * time.Minute}},
		{Name: "pg_schema_non_indexed_fkey", Stype: model.ServiceTypePostgresql, Query: pgSchemaNonIndexedFKQuery, ValueNames: []string{"exists"}, LabelNames: []string{"datname", "schemaname", "relname", "colnames", "constraint", "referenced"}, Schedule: Schedule{Interval: 5 * time.Minute}},
		{Name: "pg_schema_redundant_index", Stype: model.ServiceTypePostgresql, Query: pgSchemaRedundantIndexesQuery, ValueNames: []string{"bytes"}, LabelNames: []string{"datname", "schemaname", "relname", "indexrelname", "indexdef", "redundantdef"}, Schedule: Schedule{Interval: 5 * time.Minute}},
		{Name: "pg_schema_sequence_fullness", Stype: model.ServiceTypePostgresql, Query: pgSchemaSequencesFullnessQuery, ValueNames: []string{"ratio"}, LabelNames: []string{"datname", "schemaname", "seqname"}, Schedule: Schedule{Interval: 5 * time.Minute}},
		{Name: "pg_schema_fkey_columns_mismatch", Stype: model.ServiceTypePostgresql, Query: pgSchemaFkeyColumnsMismatch, ValueNames: []string{"exists"}, LabelNames: []string{"datname", "schemaname", "relname", "colname", "refschemaname", "refrelname", "refcolname"}, Schedule: Schedule{Interval: 5 * time.Minute}},
		// system metrics are always oneshot, there is no 'database' entity
		{Name: "node_cpu_usage", Stype: model.ServiceTypeSystem, ValueNames: []string{"time"}, LabelNames: []string{"mode"}},
		{Name: "node_diskstats", Stype: model.ServiceTypeSystem, ValueNames: diskstatsValueNames, LabelNames: []string{"device"}},
		{Name: "node_netdev", Stype: model.ServiceTypeSystem, ValueNames: netdevValueNames, LabelNames: []string{"interface"}},
		{Name: "node_memory", Stype: model.ServiceTypeSystem, ValueNames: []string{"usage_bytes"}, LabelNames: []string{"usage"}},
		{Name: "node_filesystem", Stype: model.ServiceTypeSystem, ValueNames: []string{"bytes", "inodes"}, LabelNames: []string{"usage", "device", "mountpoint", "flags"}},
		{Name: "node_settings", Stype: model.ServiceTypeSystem, ValueNames: []string{"sysctl"}, LabelNames: []string{"sysctl"}, Schedule: Schedule{Interval: 5 * time.Minute}},
		{Name: "node_hardware_cores", Stype: model.ServiceTypeSystem, ValueNames: []string{"total"}, LabelNames: []string{"state"}, Schedule: Schedule{Interval: 5 * time.Minute}},
		{Name: "node_hardware_scaling_governors", Stype: model.ServiceTypeSystem, ValueNames: []string{"total"}, LabelNames: []string{"governor"}, Schedule: Schedule{Interval: 5 * time.Minute}},
		{Name: "node_hardware_numa", Stype: model.ServiceTypeSystem, ValueNames: []string{"nodes"}, Schedule: Schedule{Interval: 5 * time.Minute}},
		{Name: "node_hardware_storage_rotational", Stype: model.ServiceTypeSystem, LabelNames: []string{"device", "scheduler"}, Schedule: Schedule{Interval: 5 * time.Minute}},
		{Name: "node_uptime_seconds", Stype: model.ServiceTypeSystem},
		// pgbouncer metrics are always oneshot, there is only one 'database' entity
		{Name: "pgbouncer_pool", Stype: model.ServiceTypePgbouncer, Query: "SHOW POOLS", ValueNames: pgbouncerPoolsVN, LabelNames: []string{"database", "user", "pool_mode"}},
		{Name: "pgbouncer_stats", Stype: model.ServiceTypePgbouncer, Query: "SHOW STATS_TOTALS", ValueNames: pgbouncerStatsVN, LabelNames: []string{"database"}},
	}
)

// adjustQueries adjusts queries depending on PostgreSQL version
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
		case "pg_schema_sequence_fullness":
			if pgVersion < 100000 {
				desc.Stype = model.ServiceTypeDisabled
			}
		}
	}
}

// NewExporter creates a new configured exporter
func NewExporter(service model.Service, repo *ServiceRepo) (*PrometheusExporter, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	var (
		itype     = service.ServiceType
		projectid = service.ProjectId
		sid       = service.ServiceId
		logger    = repo.Logger.With().Str("service", "exporter").Logger()
	)

	var e = make(map[string]*prometheus.Desc)
	for _, desc := range statdesc {
		if itype == desc.Stype {
			if len(desc.ValueNames) > 0 {
				for _, suffix := range desc.ValueNames {
					var metricName = desc.Name + "_" + suffix
					e[metricName] = prometheus.NewDesc(metricName, metricsHelp[metricName], desc.LabelNames, prometheus.Labels{"project_id": projectid, "sid": sid, "db_instance": hostname})
				}
			} else {
				e[desc.Name] = prometheus.NewDesc(desc.Name, metricsHelp[desc.Name], desc.LabelNames, prometheus.Labels{"project_id": projectid, "sid": sid, "db_instance": hostname})
			}
			// activate schedule if requested
			if repo.Config.ScheduleEnabled && desc.Schedule.Interval != 0 {
				desc.ActivateSchedule()
			}
		}
	}

	return &PrometheusExporter{Logger: logger, ServiceID: sid, AllDesc: e, ServiceRepo: *repo}, nil
}

// Describe method describes all metrics specified in the exporter
func (e *PrometheusExporter) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range e.AllDesc {
		ch <- desc
	}
}

// Collect method collects all metrics specified in the exporter
func (e *PrometheusExporter) Collect(ch chan<- prometheus.Metric) {
	var metricsCnt int

	for _, service := range e.ServiceRepo.Services {
		if e.ServiceID == service.ServiceId {
			e.Logger.Debug().Msgf("%s: start collecting metrics for %s", time.Now().Format("2006-01-02 15:04:05"), e.ServiceID)

			// в зависимости от типа экспортера делаем соотв.проверки
			switch service.ServiceType {
			case model.ServiceTypePostgresql, model.ServiceTypePgbouncer:
				metricsCnt += e.collectPgMetrics(ch, service)
			case model.ServiceTypeSystem:
				metricsCnt += e.collectSystemMetrics(ch)
			}

			// check total number of failures, if too many errors then unregister exporter
			if e.TotalFailed >= exporterFailureLimit {
				prometheus.Unregister(e)
				e.ServiceRepo.RemoveService(service.Pid)
			}
		}
	}
	e.Logger.Debug().Msgf("%s: generated %d metrics\n", time.Now().Format("2006-01-02 15:04:05"), metricsCnt)
}

// collectSystemMetrics is the wrapper for all system metrics collectors
func (e *PrometheusExporter) collectSystemMetrics(ch chan<- prometheus.Metric) (cnt int) {
	funcs := map[string]func(chan<- prometheus.Metric) int{
		"node_cpu_usage":                   e.collectCpuMetrics,
		"node_diskstats":                   e.collectDiskstatsMetrics,
		"node_netdev":                      e.collectNetdevMetrics,
		"node_memory":                      e.collectMemMetrics,
		"node_filesystem":                  e.collectFsMetrics,
		"node_settings":                    e.collectSysctlMetrics,
		"node_hardware_cores":              e.collectCpuCoresState,
		"node_hardware_scaling_governors":  e.collectCpuScalingGovernors,
		"node_hardware_numa":               e.collectNumaNodes,
		"node_hardware_storage_rotational": e.collectStorageSchedulers,
		"node_uptime_seconds":              e.collectSystemUptime,
	}

	for _, desc := range statdesc {
		if desc.Stype != model.ServiceTypeSystem {
			continue
		}
		if desc.IsScheduleActive() && !desc.IsScheduleExpired() {
			continue
		}
		// execute the method
		cnt += funcs[desc.Name](ch)
		desc.ScheduleUpdateExpired()
	}
	return cnt
}

// collectCpuMetrics collects CPU usage metrics
func (e *PrometheusExporter) collectCpuMetrics(ch chan<- prometheus.Metric) (cnt int) {
	var cpuStat stat.CpuRawstat
	cpuStat.ReadLocal()
	for _, mode := range []string{"user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal", "guest", "guest_nice", "total"} {
		ch <- prometheus.MustNewConstMetric(e.AllDesc["node_cpu_usage_time"], prometheus.CounterValue, cpuStat.SingleStat(mode), mode)
		cnt += 1
	}
	return cnt
}

// collectMemMetrics collects memory/swap usage metrics
func (e *PrometheusExporter) collectMemMetrics(ch chan<- prometheus.Metric) (cnt int) {
	var meminfoStat stat.Meminfo
	var usages = []string{"mem_total", "mem_free", "mem_used", "swap_total", "swap_free", "swap_used", "mem_cached", "mem_dirty",
		"mem_writeback", "mem_buffers", "mem_available", "mem_slab", "hp_total", "hp_free", "hp_rsvd", "hp_surp", "hp_pagesize"}
	meminfoStat.ReadLocal()
	for _, usage := range usages {
		ch <- prometheus.MustNewConstMetric(e.AllDesc["node_memory_usage_bytes"], prometheus.GaugeValue, float64(meminfoStat.SingleStat(usage)), usage)
		cnt += 1
	}
	return cnt
}

// collectDiskstatsMetrics collects block devices usage metrics
func (e *PrometheusExporter) collectDiskstatsMetrics(ch chan<- prometheus.Metric) (cnt int) {
	var diskUtilStat stat.Diskstats
	bdevCnt, err := stat.CountLinesLocal(stat.ProcDiskstats)
	if err == nil {
		diskUtilStat = make(stat.Diskstats, bdevCnt)
		err := diskUtilStat.ReadLocal()
		if err != nil {
			e.Logger.Error().Err(err).Msg("failed to collect diskstat metrics")
			return 0
		}

		for _, s := range diskUtilStat {
			if s.Rcompleted == 0 && s.Wcompleted == 0 {
				continue // skip devices which never doing IOs
			}
			for _, v := range diskstatsValueNames {
				var desc = "node_diskstats_" + v
				ch <- prometheus.MustNewConstMetric(e.AllDesc[desc], prometheus.CounterValue, s.SingleStat(v), s.Device)
				cnt += 1
			}
		}
	}
	return cnt
}

// collectNetdevMetrics collects network interfaces usage metrics
func (e *PrometheusExporter) collectNetdevMetrics(ch chan<- prometheus.Metric) (cnt int) {
	var netdevUtil stat.Netdevs
	ifsCnt, err := stat.CountLinesLocal(stat.ProcNetdev)
	if err == nil {
		netdevUtil = make(stat.Netdevs, ifsCnt)
		err := netdevUtil.ReadLocal()
		if err != nil {
			e.Logger.Error().Err(err).Msg("failed to collect netdev metrics")
			return 0
		}

		for _, s := range netdevUtil {
			if s.Rpackets == 0 && s.Tpackets == 0 {
				continue // skip interfaces which never seen packets
			}

			for _, v := range netdevValueNames {
				var desc = "node_netdev_" + v

				// TODO: вроде эти метрики не нужны -- нужны, пригодятся для 'capacity planning' проверок
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

// collectFsMetrics collects mounted filesystems' usage metrics
func (e *PrometheusExporter) collectFsMetrics(ch chan<- prometheus.Metric) (cnt int) {
	var fsStats = make(stat.FsStats, 0, 10)
	err := fsStats.ReadLocal()
	if err != nil {
		e.Logger.Error().Err(err).Msg("failed to collect filesystem metrics")
		return 0
	}

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

// collectSysctlMetrics collects sysctl metrics
func (e *PrometheusExporter) collectSysctlMetrics(ch chan<- prometheus.Metric) (cnt int) {
	for _, sysctl := range sysctlList {
		value, err := stat.GetSysctl(sysctl)
		if err != nil {
			e.Logger.Error().Err(err).Msg("failed to obtain sysctl")
			continue
		}
		ch <- prometheus.MustNewConstMetric(e.AllDesc["node_settings_sysctl"], prometheus.CounterValue, float64(value), sysctl)
		cnt += 1
	}
	return cnt
}

// collectCpuCoresState collects CPU cores operational states' metrics
func (e *PrometheusExporter) collectCpuCoresState(ch chan<- prometheus.Metric) (cnt int) {
	// Collect total number of CPU cores
	online, offline, err := stat.CountCpu()
	if err != nil {
		e.Logger.Error().Err(err).Msg("failed counting CPUs")
		return 0
	}
	total := online + offline
	for state, v := range map[string]int{"all": total, "online": online, "offline": offline} {
		ch <- prometheus.MustNewConstMetric(e.AllDesc["node_hardware_cores_total"], prometheus.CounterValue, float64(v), state)
		cnt++
	}
	return cnt
}

// collectCpuScalingGovernors collects metrics about CPUs scaling governors
func (e *PrometheusExporter) collectCpuScalingGovernors(ch chan<- prometheus.Metric) (cnt int) {
	sg, err := stat.CountScalingGovernors()
	if err != nil {
		e.Logger.Error().Err(err).Msg("failed counting scaling governors")
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
func (e *PrometheusExporter) collectNumaNodes(ch chan<- prometheus.Metric) (cnt int) {
	numa, err := stat.CountNumaNodes()
	if err != nil {
		e.Logger.Error().Err(err).Msg("failed counting NUMA nodes")
		return 0
	}
	ch <- prometheus.MustNewConstMetric(e.AllDesc["node_hardware_numa_nodes"], prometheus.CounterValue, float64(numa))
	cnt++
	return cnt
}

// collectStorageSchedulers collect metrics about attached block devices, such as HDD, SSD, NVMe, etc.
func (e *PrometheusExporter) collectStorageSchedulers(ch chan<- prometheus.Metric) (cnt int) {
	dirs, err := filepath.Glob("/sys/block/*")
	if err != nil {
		fmt.Println(err)
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
				e.Logger.Warn().Err(err)
				continue
			}
			scheduler, err = stat.GetDeviceScheduler(devpath)
			if err != nil {
				e.Logger.Warn().Err(err)
				continue
			}
			ch <- prometheus.MustNewConstMetric(e.AllDesc["node_hardware_storage_rotational"], prometheus.GaugeValue, rotational, devname, scheduler)
			cnt++
		}
	}
	return cnt
}

// collectSystemUptime collects metric about system uptime
func (e *PrometheusExporter) collectSystemUptime(ch chan<- prometheus.Metric) (cnt int) {
	uptime, err := stat.Uptime()
	if err != nil {
		fmt.Println(err)
		return 0
	}
	ch <- prometheus.MustNewConstMetric(e.AllDesc["node_uptime_seconds"], prometheus.CounterValue, uptime)
	return 1
}

// collectPgMetrics collects metrics about PostgreSQL
// Собираем стату постгреса или баунсера.
// Сначала определяем тип экспортера и соотв. какую стату хотим собрать -- баунсерную или постгресовую.
// В случае постгресовой статы, в начале подключаемся к постгресу и собираем список баз (далее мы будем подключаться к этим базам и собирать стату по объектам базы)
// В любом случае формируем т.н. список баз, в самом простом случае там будет как минимум 1 имя базы - дефолтное имя обнаржуенное при авто-дискавери
// После того как список сформирован, создаем хранилище для собранных данных куда будет складываться все данные собранные от постгреса или баунсера. На основе данных из этого хранилища будем генерить метрики для прометеуса
// Начинаем с того что проходимся в цикле по списку баз, устанавливаем соединение с этой базой, смотрим ее версию, адаптируем запросы под конкретную версию и запускаем сбор статы.
// При первой итерации сбора статы всегда собираем всю стату - и шаредную и приватную. После сбора закрываем соединение.
// После того как стата собрана, на основе данных хранилища формируем метрики для прометеуса. Учитывая что шаредная стата уже собрана, в последующих циклам собираем только приватную стату. И так пока на дойдем до конца списка баз
func (e *PrometheusExporter) collectPgMetrics(ch chan<- prometheus.Metric, service model.Service) (cnt int) {
	var dblist []string
	var version int // version of Postgres or Pgbouncer or whatever else

	// формируем список баз -- как минимум в этот список будет входить база из автодискавери
	if service.ServiceType == model.ServiceTypePostgresql {
		conn, err := CreateConn(&service)
		if err != nil {
			e.TotalFailed++
			e.Logger.Warn().Err(err).Msgf("collect failed: %d/%d, skip collecting stats for %s, failed to connect", e.TotalFailed, exporterFailureLimit, service.ServiceId)
			return 0
		}
		if err := PQstatus(conn, service.ServiceType); err != nil {
			e.TotalFailed++
			e.Logger.Warn().Err(err).Msgf("collect failed: %d/%d, skip collecting stats for %s, failed to check status", e.TotalFailed, exporterFailureLimit, service.ServiceId)
			return 0
		}
		// адаптируем запросы под конкретную версию
		if err := conn.QueryRow(pgVersionNumQuery).Scan(&version); err != nil {
			e.Logger.Warn().Err(err).Msgf("skip collecting stats for %s, failed to obtain postgresql version", service.ServiceId)
			return 0
		}
		adjustQueries(statdesc, version)

		dblist, err = getDBList(conn)
		if err != nil {
			e.Logger.Warn().Err(err).Msgf("failed to get list of databases, use default database name: %s", service.Dbname)
			dblist = []string{service.Dbname}
		}

		if err := conn.Close(); err != nil {
			e.Logger.Warn().Err(err).Msgf("failed to close the connection %s@%s:%d/%s, ignore", service.User, service.Host, service.Port, service.Dbname)
		}
	} else {
		dblist = []string{"pgbouncer"}
	}

	// Before start the collecting, resetting all 'collectDone' flags
	for _, desc := range statdesc {
		desc.collectDone = false
	}

	// Run collecting round, go through databases and collect required statistics
	for _, dbname := range dblist {
		service.Dbname = dbname

		conn, err := CreateConn(&service) // открываем коннект к базе
		if err != nil {
			e.TotalFailed++
			e.Logger.Warn().Err(err).Msgf("collect failed: %d/%d, skip collecting stats for database %s/%s, failed to connect", e.TotalFailed, exporterFailureLimit, service.ServiceId, dbname)
			continue
		}

		// собираем стату БД, в зависимости от типа это может быть баунсерная или постгресовая стата
		e.getDBStat(conn, ch, service.ServiceType, version)
		if err := conn.Close(); err != nil {
			e.Logger.Warn().Err(err).Msgf("failed to close the connection %s@%s:%d/%s", service.User, service.Host, service.Port, service.Dbname)
		}
	}
	// After collecting, update expired schedules. Don't update schedules inside the collecting round, because that might cancel collecting non-oneshot statistics
	for _, desc := range statdesc {
		if desc.collectDone {
			desc.ScheduleUpdateExpired()
		}
	}
	return cnt
}

// getDBStat collects metrics from the connected database
// задача функции собрать стату в зависимости от потребности - шаредную или приватную.
// Шаредная стата описывает кластер целиком, приватная относится к конкретной базе и описывает таблицы/индексы/функции которые принадлежат этой базе
// Для сбора статы обходим все имеющиеся источники и пропускаем ненужные. Далее выполняем запрос ассоциированный с источником и делаем его в подключение.
// Полученный ответ от базы оформляем в массив данных и складываем в общее хранилище в котором собраны данные от всех ответов, когда все источники обшарены возвращаем наружу общее хранилище с собранными данными
func (e *PrometheusExporter) getDBStat(conn *sql.DB, ch chan<- prometheus.Metric, itype int, version int) {
	// обходим по всем источникам
	for _, desc := range statdesc {
		if desc.Stype != itype {
			continue
		}
		// Check the schedule, skip if not expired
		if desc.IsScheduleActive() && !desc.IsScheduleExpired() {
			continue
		}
		// Skip collecting if statistics is oneshot and already collected (in the previous database)
		if desc.collectDone && desc.collectOneshot {
			continue
		}

		e.Logger.Debug().Msgf("start collecting %s", desc.Name)

		// обрабатываем статки с пустым запросом
		if desc.Query == "" {
			if err := getPostgresDirInfo(e, conn, ch, desc.Name, version); err != nil {
				e.Logger.Warn().Err(err).Msgf("skip collecting %s", desc.Name)
			} else {
				desc.ScheduleUpdateExpired()
				desc.collectDone = true
			}
			continue
		}

		// check pg_stat_statements availability in this database
		if desc.Name == "pg_stat_statements" && !IsPGSSAvailable(conn) {
			e.Logger.Debug().Msg("skip collecting pg_stat_statements in this database")
			continue
		}

		rows, err := conn.Query(desc.Query)
		if err != nil {
			e.Logger.Warn().Err(err).Msgf("skip collecting %s, failed to execute query", desc.Name)
			continue
		}

		var container []sql.NullString
		var pointers []interface{}

		colnames, _ := rows.Columns()
		ncols := len(colnames)

		var noRows = true
		for rows.Next() {
			noRows = false
			pointers = make([]interface{}, ncols)
			container = make([]sql.NullString, ncols)

			for i := range pointers {
				pointers[i] = &container[i]
			}

			err := rows.Scan(pointers...)
			if err != nil {
				e.Logger.Warn().Err(err).Msgf("skip collecting %s, failed to scan query result", desc.Name)
				continue // если произошла ошибка, то пропускаем эту строку и переходим к следующей
			}

			for c, colname := range colnames {
				// Если колонки нет в списке меток, то генерим метрику на основе значения [row][column].
				// Если имя колонки входит в список меток, то пропускаем ее -- нам не нужно генерить из нее метрику, т.к. она как метка+значение сама будет частью метрики
				if !Contains(desc.LabelNames, colname) {
					var labelValues = make([]string, len(desc.LabelNames))

					// итерируемся по именам меток, нужно собрать из результата-ответа от базы, значения для соотв. меток
					for i, lname := range desc.LabelNames {
						// определяем номер (индекс) колонки в PGresult, который соотв. названию метки -- по этому индексу возьмем значение для метки из PGresult
						// (таким образом мы не привязываемся к порядку полей в запросе)
						for idx, cname := range colnames {
							if cname == lname {
								labelValues[i] = container[idx].String
							}
						}
					}

					// игнорируем пустые строки, это NULL - нас они не интересуют
					if container[c].String == "" {
						e.Logger.Debug().Msgf("skip collecting %s_%s metric: got empty value", desc.Name, colname)
						continue
					}

					// получаем значение метрики (string) и конвертим его в подходящий для прометеуса float64
					v, err := strconv.ParseFloat(container[c].String, 64)
					if err != nil {
						e.Logger.Warn().Err(err).Msgf("skip collecting %s_%s metric", desc.Name, colname)
						continue
					}

					// отправляем метрику в прометеус
					ch <- prometheus.MustNewConstMetric(
						e.AllDesc[desc.Name+"_"+colname], // *prometheus.Desc который также участвует в Describe методе
						prometheus.CounterValue,          // тип метрики
						v,                                // значение метрики
						labelValues...,                   // массив меток
					)
				}
			}
		}
		if err := rows.Close(); err != nil {
			e.Logger.Debug().Err(err).Msg("failed to close rows, ignore")
		}
		if noRows {
			e.Logger.Debug().Msgf("no rows returned for %s", desc.Name)
			continue
		}
		desc.collectDone = true
		e.TotalFailed = 0
		e.Logger.Debug().Msgf("%s collected", desc.Name)
	}
}

// IsPGSSAvailable returns true if pg_stat_statements exists and available
func IsPGSSAvailable(conn *sql.DB) bool {
	log.Debug().Msg("check pg_stat_statements availability")
	/* check pg_stat_statements */
	var pgCheckPGSSExists = `SELECT EXISTS (SELECT 1 FROM information_schema.views WHERE table_name = 'pg_stat_statements')`
	var pgCheckPGSSCount = `SELECT 1 FROM pg_stat_statements LIMIT 1`
	var vExists bool
	var vCount int
	if err := conn.QueryRow(pgCheckPGSSExists).Scan(&vExists); err != nil {
		log.Debug().Msg("failed to check pg_stat_statements view in information_schema")
		return false // failed to query information_schema
	}
	if vExists == false {
		log.Debug().Msg("pg_stat_statements is not available in this database")
		return false // pg_stat_statements is not available
	}
	if err := conn.QueryRow(pgCheckPGSSCount).Scan(&vCount); err != nil {
		log.Debug().Msg("pg_stat_statements exists but not queryable")
		return false // view exists, but unavailable for queries - empty shared_preload_libraries ?
	}
	return true
}

// getPostgresDirInfo evaluates mountpoint of Postgres directory
func getPostgresDirInfo(e *PrometheusExporter, conn *sql.DB, ch chan<- prometheus.Metric, target string, version int) (err error) {
	var dirpath string
	if err := conn.QueryRow(`SELECT current_setting('data_directory')`).Scan(&dirpath); err != nil {
		return err
	}
	switch target {
	case "pg_wal_directory":
		if version >= 100000 {
			dirpath = dirpath + "/pg_wal"
		} else {
			dirpath = dirpath + "/pg_xlog"
		}
	case "pg_log_directory":
		var logpath string
		if err := conn.QueryRow(`SELECT current_setting('log_directory') WHERE current_setting('logging_collector') = 'on'`).Scan(&logpath); err != nil {
			return err
		}
		if strings.HasPrefix(logpath, "/") {
			dirpath = logpath
		} else {
			dirpath = dirpath + "/" + logpath
		}
	}

	mountpoints := stat.ReadMounts()
	realpath, err := stat.RewritePath(dirpath)
	if err != nil {
		return err
	}

	parts := strings.Split(realpath, "/")
	for i := len(parts); i > 0; i-- {
		if subpath := strings.Join(parts[0:i], "/"); subpath != "" {
			// check is subpath a symlink? if symlink - dereference and replace it
			fi, err := os.Lstat(subpath)
			if err != nil {
				return err
			}
			if fi.Mode()&os.ModeSymlink != 0 {
				resolvedLink, err := os.Readlink(subpath)
				if err != nil {
					return fmt.Errorf("failed to resolve symlink %s: %s\n", subpath, err)
				}

				if _, ok := mountpoints[resolvedLink]; ok {
					subpath = resolvedLink
				}
			}
			if device, ok := mountpoints[subpath]; ok {
				ch <- prometheus.MustNewConstMetric(e.AllDesc[target], prometheus.GaugeValue, 1, device, subpath, realpath)
				return nil
			}
		} else {
			device := mountpoints["/"]
			ch <- prometheus.MustNewConstMetric(e.AllDesc[target], prometheus.GaugeValue, 1, device, "/", realpath)
			return nil
		}
	}
	return nil
}
