package app

import "github.com/prometheus/client_golang/prometheus"

// statDescriptor is the statistics descriptor, with detailed info about particular kind of stats
type statDescriptor struct {
	Name           string                          // имя источника откуда берется стата, выбирается произвольно и может быть как именем вьюхи, таблицы, функции, так и каким-то придуманным
	StatType       string                          // тип источника статы - постгрес, баунсер, система и т.п.
	QueryText      string                          // запрос с помощью которого вытягивается стата из источника
	ValueNames     []string                        // названия полей которые будут использованы как значения метрик
	ValueTypes     map[string]prometheus.ValueType //теоретически мапа нужна для хренения карты метрика <-> тип, например xact_commit <-> Counter/Gauge. Но пока поле не используется никак
	LabelNames     []string                        // названия полей которые будут использованы как метки
	collectDone    bool                            // стата уже собрана (для всяких шаредных стат типа pg_stat_bgwriter, pg_stat_database)
	collectOneshot bool                            // стату собирать только один раз за раунд, (например всякие шаредные статы тип pg_stat_database)
	Schedule                                       // расписание по которому осуществляется сбор метрик
}

// statCatalog is the set of statDescriptors
type statCatalog []statDescriptor

// diskstatsValueNames returns fields of diskstat
func diskstatsValueNames() []string {
	return []string{"rcompleted", "rmerged", "rsectors", "rspent", "wcompleted", "wmerged", "wsectors", "wspent", "ioinprogress", "tspent", "tweighted", "uptime"}
}

// netdevValueNames returns fields of netdev stats
func netdevValueNames() []string {
	return []string{"rbytes", "rpackets", "rerrs", "rdrop", "rfifo", "rframe", "rcompressed", "rmulticast", "tbytes", "tpackets", "terrs", "tdrop", "tfifo", "tcolls", "tcarrier", "tcompressed", "saturation", "uptime", "speed", "duplex"}
}

// sysctlList returns sysctl list
func sysctlList() []string {
	return []string{"kernel.sched_migration_cost_ns", "kernel.sched_autogroup_enabled",
		"vm.dirty_background_bytes", "vm.dirty_bytes", "vm.overcommit_memory", "vm.overcommit_ratio", "vm.swappiness", "vm.min_free_kbytes",
		"vm.zone_reclaim_mode", "kernel.numa_balancing", "vm.nr_hugepages", "vm.nr_overcommit_hugepages"}
}

// globalStatCatalog provides catalog with all available statistics
func globalStatCatalog() []statDescriptor {
	var (
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
	)

	return []statDescriptor{
		// collect oneshot -- these Postgres statistics are collected once per round
		{Name: "pg_stat_database", StatType: "postgres", QueryText: pgStatDatabaseQuery, collectOneshot: true, ValueNames: pgStatDatabasesValueNames, LabelNames: []string{"datid", "datname"}},
		{Name: "pg_stat_bgwriter", StatType: "postgres", QueryText: pgStatBgwriterQuery, collectOneshot: true, ValueNames: pgStatBgwriterValueNames, LabelNames: []string{}},
		{Name: "pg_stat_user_functions", StatType: "postgres", QueryText: pgStatUserFunctionsQuery, ValueNames: pgStatUserFunctionsValueNames, LabelNames: []string{"funcid", "datname", "schemaname", "funcname"}},
		{Name: "pg_stat_activity", StatType: "postgres", QueryText: pgStatActivityQuery, collectOneshot: true, ValueNames: pgStatActivityValueNames, LabelNames: []string{}},
		{Name: "pg_stat_activity", StatType: "postgres", QueryText: pgStatActivityDurationsQuery, collectOneshot: true, ValueNames: pgStatActivityDurationsNames, LabelNames: []string{}},
		{Name: "pg_stat_activity_autovac", StatType: "postgres", QueryText: pgStatActivityAutovacQuery, collectOneshot: true, ValueNames: pgStatActivityAutovacValueNames, LabelNames: []string{}},
		{Name: "pg_stat_statements", StatType: "postgres", QueryText: pgStatStatementsQuery, collectOneshot: true, ValueNames: pgStatStatementsValueNames, LabelNames: []string{"usename", "datname", "queryid", "query"}},
		{Name: "pg_stat_replication", StatType: "postgres", QueryText: pgStatReplicationQuery, collectOneshot: true, ValueNames: pgStatReplicationValueNames, LabelNames: []string{"client_addr", "application_name"}},
		{Name: "pg_replication_slots_restart_lag", StatType: "postgres", QueryText: pgReplicationSlotsQuery, collectOneshot: true, ValueNames: []string{"bytes"}, LabelNames: []string{"slot_name", "active"}},
		{Name: "pg_replication_slots", StatType: "postgres", QueryText: pgReplicationSlotsCountQuery, collectOneshot: true, ValueNames: []string{"conn"}, LabelNames: []string{"state"}},
		{Name: "pg_replication_standby", StatType: "postgres", QueryText: pgReplicationStandbyCount, collectOneshot: true, ValueNames: []string{"count"}, LabelNames: []string{}},
		{Name: "pg_recovery", StatType: "postgres", QueryText: pgRecoveryStatusQuery, collectOneshot: true, ValueNames: []string{"status"}},
		{Name: "pg_stat_database_conflicts", StatType: "postgres", QueryText: pgStatDatabaseConflictsQuery, collectOneshot: true, ValueNames: pgStatDatabaseConflictsValueNames, LabelNames: []string{}},
		{Name: "pg_stat_basebackup", StatType: "postgres", QueryText: pgStatBasebackupQuery, collectOneshot: true, ValueNames: []string{"count", "duration_seconds_max"}, LabelNames: []string{}},
		{Name: "pg_stat_current_temp", StatType: "postgres", QueryText: pgStatCurrentTempFilesQuery, collectOneshot: true, ValueNames: pgStatCurrentTempFilesVN, LabelNames: []string{"tablespace"}},
		{Name: "pg_data_directory", StatType: "postgres", QueryText: "", collectOneshot: true, LabelNames: []string{"device", "mountpoint", "path"}, Schedule: newSchedule(defaultScheduleInterval)},
		{Name: "pg_wal_directory", StatType: "postgres", QueryText: "", collectOneshot: true, LabelNames: []string{"device", "mountpoint", "path"}, Schedule: newSchedule(defaultScheduleInterval)},
		{Name: "pg_log_directory", StatType: "postgres", QueryText: "", collectOneshot: true, LabelNames: []string{"device", "mountpoint", "path"}, Schedule: newSchedule(defaultScheduleInterval)},
		{Name: "pg_wal_directory", StatType: "postgres", QueryText: pgStatWalSizeQuery, collectOneshot: true, ValueNames: []string{"size_bytes"}, LabelNames: []string{}, Schedule: newSchedule(defaultScheduleInterval)},
		{Name: "pg_log_directory", StatType: "postgres", QueryText: pgLogdirSizeQuery, collectOneshot: true, ValueNames: []string{"size_bytes"}, LabelNames: []string{}, Schedule: newSchedule(defaultScheduleInterval)},
		{Name: "pg_catalog_size", StatType: "postgres", QueryText: pgCatalogSizeQuery, ValueNames: []string{"bytes"}, LabelNames: []string{"datname"}, Schedule: newSchedule(defaultScheduleInterval)},
		{Name: "pg_settings", StatType: "postgres", QueryText: pgSettingsGucQuery, collectOneshot: true, ValueNames: []string{"guc"}, LabelNames: []string{"name", "unit", "secondary"}, Schedule: newSchedule(defaultScheduleInterval)},
		// collect always -- these Postgres statistics are collected every time in all databases
		{Name: "pg_stat_user_tables", StatType: "postgres", QueryText: pgStatUserTablesQuery, ValueNames: pgStatUserTablesValueNames, LabelNames: []string{"datname", "schemaname", "relname"}},
		{Name: "pg_statio_user_tables", StatType: "postgres", QueryText: pgStatioUserTablesQuery, ValueNames: pgStatioUserTablesValueNames, LabelNames: []string{"datname", "schemaname", "relname"}},
		{Name: "pg_stat_user_indexes", StatType: "postgres", QueryText: pgStatUserIndexesQuery, ValueNames: pgStatUserIndexesValueNames, LabelNames: []string{"datname", "schemaname", "relname", "indexrelname"}},
		{Name: "pg_statio_user_indexes", StatType: "postgres", QueryText: pgStatioUserIndexesQuery, ValueNames: pgStatioUserIndexesValueNames, LabelNames: []string{"datname", "schemaname", "relname", "indexrelname"}},
		{Name: "pg_schema_non_pk_table", StatType: "postgres", QueryText: pgSchemaNonPrimaryKeyTablesQuery, ValueNames: []string{"exists"}, LabelNames: []string{"datname", "schemaname", "relname"}, Schedule: newSchedule(defaultScheduleInterval)},
		{Name: "pg_schema_invalid_index", StatType: "postgres", QueryText: pgSchemaInvalidIndexesQuery, ValueNames: []string{"bytes"}, LabelNames: []string{"datname", "schemaname", "relname", "indexrelname"}, Schedule: newSchedule(defaultScheduleInterval)},
		{Name: "pg_schema_non_indexed_fkey", StatType: "postgres", QueryText: pgSchemaNonIndexedFKQuery, ValueNames: []string{"exists"}, LabelNames: []string{"datname", "schemaname", "relname", "colnames", "constraint", "referenced"}, Schedule: newSchedule(defaultScheduleInterval)},
		{Name: "pg_schema_redundant_index", StatType: "postgres", QueryText: pgSchemaRedundantIndexesQuery, ValueNames: []string{"bytes"}, LabelNames: []string{"datname", "schemaname", "relname", "indexrelname", "indexdef", "redundantdef"}, Schedule: newSchedule(defaultScheduleInterval)},
		{Name: "pg_schema_sequence_fullness", StatType: "postgres", QueryText: pgSchemaSequencesFullnessQuery, ValueNames: []string{"ratio"}, LabelNames: []string{"datname", "schemaname", "seqname"}, Schedule: newSchedule(defaultScheduleInterval)},
		{Name: "pg_schema_fkey_columns_mismatch", StatType: "postgres", QueryText: pgSchemaFkeyColumnsMismatch, ValueNames: []string{"exists"}, LabelNames: []string{"datname", "schemaname", "relname", "colname", "refschemaname", "refrelname", "refcolname"}, Schedule: newSchedule(defaultScheduleInterval)},
		// system metrics are always oneshot, there is no 'database' entity
		{Name: "node_cpu_usage", StatType: "system", ValueNames: []string{"time"}, LabelNames: []string{"mode"}},
		{Name: "node_diskstats", StatType: "system", ValueNames: diskstatsValueNames(), LabelNames: []string{"device"}},
		{Name: "node_netdev", StatType: "system", ValueNames: netdevValueNames(), LabelNames: []string{"interface"}},
		{Name: "node_memory", StatType: "system", ValueNames: []string{"usage_bytes"}, LabelNames: []string{"usage"}},
		{Name: "node_filesystem", StatType: "system", ValueNames: []string{"bytes", "inodes"}, LabelNames: []string{"usage", "device", "mountpoint", "flags"}},
		{Name: "node_settings", StatType: "system", ValueNames: []string{"sysctl"}, LabelNames: []string{"sysctl"}, Schedule: newSchedule(defaultScheduleInterval)},
		{Name: "node_hardware_cores", StatType: "system", ValueNames: []string{"total"}, LabelNames: []string{"state"}, Schedule: newSchedule(defaultScheduleInterval)},
		{Name: "node_hardware_scaling_governors", StatType: "system", ValueNames: []string{"total"}, LabelNames: []string{"governor"}, Schedule: newSchedule(defaultScheduleInterval)},
		{Name: "node_hardware_numa", StatType: "system", ValueNames: []string{"nodes"}, Schedule: newSchedule(defaultScheduleInterval)},
		{Name: "node_hardware_storage_rotational", StatType: "system", LabelNames: []string{"device", "scheduler"}, Schedule: newSchedule(defaultScheduleInterval)},
		{Name: "node_uptime_seconds", StatType: "system"},
		// pgbouncer metrics are always oneshot, there is only one 'database' entity
		{Name: "pgbouncer_pool", StatType: "pgbouncer", QueryText: "SHOW POOLS", ValueNames: pgbouncerPoolsVN, LabelNames: []string{"database", "user", "pool_mode"}},
		{Name: "pgbouncer_stats", StatType: "pgbouncer", QueryText: "SHOW STATS_TOTALS", ValueNames: pgbouncerStatsVN, LabelNames: []string{"database"}},
	}
}
