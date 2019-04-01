//
package main

var (
	pgVersionNumQuery = "SELECT current_setting('server_version_num')"
	//pgGetSysIdQuery   = "SELECT system_identifier FROM pg_control_system()"

	pgStatDatabaseQuery      = "SELECT datid, datname, xact_commit, xact_rollback, blks_read, blks_hit, tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted, conflicts, temp_files, temp_bytes, deadlocks, blk_read_time, blk_write_time, pg_database_size(datname) as db_size, coalesce(extract('epoch' from age(now(), stats_reset)), 0) as stats_age_seconds FROM pg_stat_database WHERE datname IN (SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate)"
	pgStatUserTablesQuery    = "SELECT current_database() AS datname, schemaname, relname, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd, n_live_tup, n_dead_tup, n_mod_since_analyze, vacuum_count, autovacuum_count, analyze_count, autoanalyze_count FROM pg_stat_user_tables"
	pgStatioUserTablesQuery  = "SELECT current_database() AS datname, schemaname, relname, heap_blks_read, heap_blks_hit, idx_blks_read, idx_blks_hit, toast_blks_read, toast_blks_hit, tidx_blks_read, tidx_blks_hit FROM pg_statio_user_tables"
	pgStatUserIndexesQuery   = "SELECT current_database() AS datname, schemaname, relname, indexrelname, idx_scan, idx_tup_read, idx_tup_fetch FROM pg_stat_user_indexes"
	pgStatioUserIndexesQuery = "SELECT current_database() AS datname, schemaname, relname, indexrelname, idx_blks_read, idx_blks_hit FROM pg_statio_user_indexes"
	pgStatBgwriterQuery      = "select checkpoints_timed, checkpoints_req, checkpoint_write_time, checkpoint_sync_time, buffers_checkpoint, buffers_clean, maxwritten_clean, buffers_backend, buffers_backend_fsync, buffers_alloc from pg_stat_bgwriter"
	pgStatUserFunctionsQuery = "SELECT funcid, current_database() AS datname, schemaname, funcname, calls, total_time, self_time FROM pg_stat_user_functions"

	pgStatActivityQuery = `SELECT
    					count(*) FILTER (WHERE state IS NOT NULL) AS conn_total,
    					count(*) FILTER (WHERE state = 'idle') AS conn_idle_total,
    					count(*) FILTER (WHERE state IN ('idle in transaction', 'idle in transaction (aborted)')) AS conn_idle_xact_total,
    					count(*) FILTER (WHERE state = 'active') AS conn_active_total,
    					count(*) FILTER (WHERE wait_event_type = 'Lock') AS conn_waiting_total,
    					count(*) FILTER (WHERE state IN ('fastpath function call','disabled')) AS conn_others_total,
    					(SELECT count(*) FROM pg_prepared_xacts) AS conn_prepared_total,
    					coalesce(extract(epoch from max(clock_timestamp() - coalesce(xact_start, query_start)) FILTER (WHERE (query !~* '^autovacuum:' AND query !~* '^vacuum' AND state != 'idle') AND pid <> pg_backend_pid())), 0) AS xact_max_duration
					FROM pg_stat_activity`

	pgStatActivityAutovacQuery = `SELECT
						count(*) FILTER (WHERE query ~* '^autovacuum:') AS workers_total,
						count(*) FILTER (WHERE query ~* '^autovacuum:.*to prevent wraparound') AS antiwraparound_workers_total,
						count(*) FILTER (WHERE query ~ '^vacuum' AND state != 'idle') AS user_vacuum_total,
						coalesce(extract(epoch from max(clock_timestamp() - coalesce(xact_start, query_start))), 0) AS max_duration
					FROM pg_stat_activity
					WHERE (query ~* '^autovacuum:' OR query ~* '^vacuum') AND pid <> pg_backend_pid()`

	pgStatReplicationQuery96 = `SELECT coalesce(client_addr, '127.0.0.1') AS client_addr,
					application_name,
					(case pg_is_in_recovery() when 't' then null else pg_xlog_location_diff(pg_current_xlog_location(), '0/00000000') end) AS pg_wal_bytes,
					pg_xlog_location_diff(pg_current_xlog_location(), sent_location) AS pending_lag_bytes,
					pg_xlog_location_diff(sent_location, write_location) AS write_lag_bytes,
					pg_xlog_location_diff(write_location, flush_location) AS flush_lag_bytes,
					pg_xlog_location_diff(flush_location, replay_location) AS replay_lag_bytes,
					pg_xlog_location_diff(pg_current_xlog_location(), replay_location) AS total_lag_bytes
				FROM pg_stat_replication WHERE state != 'backup' AND application_name != 'pg_basebackup'`

	pgStatReplicationQuery = `SELECT coalesce(client_addr, '127.0.0.1') AS client_addr,
					application_name,
					(case pg_is_in_recovery() when 't' then null else pg_wal_lsn_diff(pg_current_wal_lsn(), '0/00000000') end) AS pg_wal_bytes,
					pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn) AS pending_lag_bytes,
					pg_wal_lsn_diff(sent_lsn, write_lsn) AS write_lag_bytes,
					pg_wal_lsn_diff(write_lsn, flush_lsn) AS flush_lag_bytes,
					pg_wal_lsn_diff(flush_lsn, replay_lsn) AS replay_lag_bytes,
					pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS total_lag_bytes,
					extract(epoch from write_lag) as write_lag_sec,
					extract(epoch from flush_lag) as flush_lag_sec,
					extract(epoch from replay_lag) as replay_lag_sec
				FROM pg_stat_replication WHERE state != 'backup' AND application_name != 'pg_basebackup'`

	pgReplicationSlotsQuery96 = `SELECT slot_name, active::int,pg_xlog_location_diff(pg_current_xlog_location(), restart_lsn) AS bytes FROM pg_replication_slots`

	pgReplicationSlotsQuery = `SELECT slot_name, active::int,pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS bytes FROM pg_replication_slots`

	pgReplicationStandbyCount = `SELECT count(1) FROM pg_stat_replication WHERE state != 'backup' AND application_name != 'pg_basebackup'`

	pgStatBasebackupQuery = "SELECT count(pid) AS count, coalesce(extract(epoch from max(clock_timestamp() - backend_start)), 0) AS duration_seconds_max FROM pg_stat_replication WHERE state = 'backup'"

	pgRecoveryStatusQuery = `SELECT pg_is_in_recovery()::int AS status`

	pgStatDatabaseConflictsQuery = `SELECT sum(confl_tablespace + confl_lock + confl_snapshot + confl_bufferpin + confl_deadlock) AS total,
			sum(confl_tablespace) AS tablespace, sum(confl_lock) AS lock, sum(confl_snapshot) AS snapshot,
			sum(confl_bufferpin) AS bufferpin, sum(confl_deadlock) AS deadlock FROM pg_stat_database_conflicts`

	pgReplicationSlotsCountQuery = `SELECT 'total' as state, count(*) AS conn FROM pg_replication_slots
		UNION SELECT 'active' AS state, count(*) filter (where active) AS conn FROM pg_replication_slots
		UNION SELECT 'inactive' AS state, count(*) filter (where not active) AS conn FROM pg_replication_slots`

	pgStatStatementsQuery = `SELECT
					    pg_get_userbyid(p.userid) AS usename, d.datname AS datname, p.queryid,
					    regexp_replace(left(p.query, 1024),E'\\s+', ' ', 'g') AS query,
						p.calls, p.rows,
						p.total_time, p.blk_read_time, p.blk_write_time,
    					p.shared_blks_hit, p.shared_blks_read, p.shared_blks_dirtied, p.shared_blks_written,
    					p.local_blks_hit, p.local_blks_read, p.local_blks_dirtied, p.local_blks_written,
						p.temp_blks_read, p.temp_blks_written
					FROM pg_stat_statements p
					JOIN pg_database d ON d.oid=p.dbid`

	pgStatCurrentTempFilesQuery = `WITH RECURSIVE tablespace_dirs AS (
					SELECT dirname, 'pg_tblspc/' || dirname || '/' AS path, 1 AS depth FROM pg_catalog.pg_ls_dir('pg_tblspc/', true, false) AS dirname
    				UNION ALL
    				SELECT subdir, td.path || subdir || '/', td.depth + 1 FROM tablespace_dirs AS td, pg_catalog.pg_ls_dir(td.path, true, false) AS subdir WHERE td.depth < 3
				), temp_dirs AS (
					SELECT td.path, ts.spcname AS tablespace
        				FROM tablespace_dirs AS td
        				INNER JOIN pg_catalog.pg_tablespace AS ts ON (ts.oid = substring(td.path FROM 'pg_tblspc/(\d+)')::int)
        				WHERE td.depth = 3 AND td.dirname = 'pgsql_tmp'
    				UNION ALL
    				VALUES ('base/pgsql_tmp/', 'pg_default')
				), temp_files AS (
					SELECT td.tablespace, pg_stat_file(td.path || '/' || filename, true) AS file_stat
    					FROM temp_dirs AS td
						LEFT JOIN pg_catalog.pg_ls_dir(td.path, true, false) AS filename ON true
				) SELECT tablespace,
    				count((file_stat).size) AS files_total,
    				coalesce(sum((file_stat).size)::BIGINT, 0) AS bytes_total,
    				coalesce(extract(epoch from clock_timestamp() - min((file_stat).access)), 0) AS oldest_file_age_seconds_max
				FROM temp_files GROUP BY 1`

	pgStatWalSizeQuery96 = `SELECT (SELECT count(*) FROM pg_ls_dir('pg_xlog')) * pg_size_bytes(current_setting('wal_segment_size')) as size_bytes`	// TODO: in this case 'archive_status' accounts as a segment
	pgStatWalSizeQuery   = `SELECT sum(size) AS size_bytes FROM pg_ls_waldir()`
	pgLogdirSizeQuery = `SELECT sum(size) AS size_bytes FROM (SELECT (pg_stat_file(logdir||'/'||pg_ls_dir(logdir))).size FROM current_setting('log_directory') AS logdir WHERE current_setting('logging_collector') = 'on') AS size`
	pgCatalogSizeQuery = `SELECT current_database() AS datname, sum(pg_total_relation_size(relname::regclass)) AS bytes FROM pg_stat_sys_tables WHERE schemaname = 'pg_catalog'`

	pgSettingsGucQuery = `SELECT name, unit,
CASE WHEN vartype = 'bool' THEN setting::bool::int::text
	WHEN vartype IN ('string', 'enum') THEN '-1000'::text
	ELSE setting
END AS guc,
CASE WHEN vartype IN ('string', 'enum', 'bool') THEN setting END AS secondary
FROM pg_show_all_settings()`

	pgSchemaNonPrimaryKeyTablesQuery = `SELECT current_database() AS datname, t.nspname AS schemaname, t.relname AS relname, 1 AS exists FROM (SELECT c.oid, c.relname, n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'r' AND n.nspname NOT IN('pg_catalog', 'information_schema')) AS t LEFT OUTER JOIN pg_constraint c ON c.contype = 'p' AND c.conrelid = t.oid WHERE c.conname IS NULL`
	pgSchemaInvalidIndexesQuery = `SELECT current_database() AS datname, c1.relnamespace::regnamespace AS schemaname, c2.relname AS relname, c1.relname AS indexrelname, pg_relation_size(c1.relname::regclass) AS bytes FROM pg_index i JOIN pg_class c1 ON i.indexrelid = c1.oid JOIN pg_class c2 ON i.indrelid = c2.oid WHERE NOT i.indisvalid`
	// Use LATERAL, hence working only on PG 9.6 and newer
	pgSchemaNonIndexedFKQuery = `SELECT current_database() AS datname, c.connamespace::regnamespace AS schemaname, s.relname AS relname, string_agg(a.attname, ',' ORDER BY x.n) AS colnames, c.conname AS constraint, c.confrelid::regclass AS referenced, 1 AS exists FROM pg_catalog.pg_constraint c CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS x(attnum, n) JOIN pg_catalog.pg_attribute a ON a.attnum = x.attnum AND a.attrelid = c.conrelid JOIN pg_class s ON c.conrelid = s.oid WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_index i WHERE i.indrelid = c.conrelid AND (i.indkey::smallint[])[0:cardinality(c.conkey)-1] @> c.conkey) AND c.contype = 'f' GROUP BY c.connamespace,s.relname,c.conname,c.confrelid`
	pgSchemaRedundantIndexesQuery = `WITH index_data AS (SELECT *,string_to_array(indkey::text,' ') AS key_array,array_length(string_to_array(indkey::text,' '),1) AS nkeys FROM pg_index) SELECT current_database() AS datname, c1.relnamespace::regnamespace AS schemaname, c1.relname AS relname, c2.relname AS indexrelname, pg_get_indexdef(i1.indexrelid) AS indexdef, pg_get_indexdef(i2.indexrelid) AS redundantdef, pg_relation_size(i2.indexrelid) AS bytes FROM index_data AS i1 JOIN index_data AS i2 ON i1.indrelid=i2.indrelid AND i1.indexrelid<>i2.indexrelid JOIN pg_class c1 ON i1.indrelid = c1.oid JOIN pg_class c2 ON i2.indexrelid = c2.oid WHERE (regexp_replace(i1.indpred, 'location \d+', 'location', 'g') IS NOT DISTINCT FROM regexp_replace(i2.indpred, 'location \d+', 'location', 'g')) AND (regexp_replace(i1.indexprs, 'location \d+', 'location', 'g') IS NOT DISTINCT FROM regexp_replace(i2.indexprs, 'location \d+', 'location', 'g')) AND ((i1.nkeys > i2.nkeys AND NOT i2.indisunique) OR (i1.nkeys=i2.nkeys AND ((i1.indisunique AND i2.indisunique AND (i1.indexrelid>i2.indexrelid)) OR (NOT i1.indisunique AND NOT i2.indisunique AND (i1.indexrelid>i2.indexrelid)) OR (i1.indisunique AND NOT i2.indisunique)))) AND i1.key_array[1:i2.nkeys]=i2.key_array`
	pgSchemaSequencesFullnessQuery = `SELECT current_database() AS datname, schemaname, sequencename AS seqname, coalesce(last_value, 0) / max_value::float AS ratio FROM pg_sequences`
	pgSchemaFkeyColumnsMismatch = `SELECT current_database() AS datname, c1.relnamespace::regnamespace AS schemaname, c1.relname AS relname, a1.attname||'::'||t1.typname AS colname, c2.relnamespace::regnamespace AS refschemaname, c2.relname AS refrelname, a2.attname||'::'||t2.typname AS refcolname, 1 AS exists FROM pg_constraint JOIN pg_class c1 ON c1.oid=conrelid JOIN pg_class c2 ON c2.oid=confrelid JOIN pg_attribute a1 ON a1.attnum=conkey[1] AND a1.attrelid=conrelid JOIN pg_attribute a2 ON a2.attnum=confkey[1] AND a2.attrelid=confrelid JOIN pg_type t1 ON t1.oid=a1.atttypid JOIN pg_type t2 ON t2.oid=a2.atttypid WHERE a1.atttypid<>a2.atttypid AND contype='f'`
)
