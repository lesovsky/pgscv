//
package main

var (
	pgVersionNumQuery = "SELECT current_setting('server_version_num')"
	pgGetSysIdQuery   = "SELECT system_identifier FROM pg_control_system()"

	pgStatDatabaseQuery      = "SELECT datid, datname, xact_commit, xact_rollback, blks_read, blks_hit, tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted, conflicts, temp_files, temp_bytes, deadlocks, blk_read_time, blk_write_time, pg_database_size(datname) as db_size FROM pg_stat_database"
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
    					coalesce(extract(epoch from max(now() - coalesce(xact_start, query_start)) FILTER (WHERE (query !~* '^autovacuum:' AND query !~* '^vacuum' AND state != 'idle') AND pid <> pg_backend_pid())), 0) AS xact_max_duration
					FROM pg_stat_activity`

	pgStatActivityAutovacQuery = `SELECT
						count(*) FILTER (WHERE query ~* '^autovacuum:') AS workers_total,
						count(*) FILTER (WHERE query ~* '^autovacuum:.*to prevent wraparound') AS antiwraparound_workers_total,
						count(*) FILTER (WHERE query ~ '^vacuum' AND state != 'idle') AS user_vacuum_total,
						coalesce(extract(epoch from max(now() - coalesce(xact_start, query_start))), 0) AS max_duration
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
				FROM pg_stat_replication`

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
				FROM pg_stat_replication`

	pgStatStatementsQuery = `SELECT
					    a.rolname AS usename, d.datname AS datname, p.queryid,
					    regexp_replace(regexp_replace(left(p.query, 512),E'( |\t)+',' ','g'),E'\n', '', 'g') AS query,
    					p.calls, p.total_time, p.rows,
    					p.shared_blks_hit, p.shared_blks_read, p.shared_blks_dirtied, p.shared_blks_written,
    					p.local_blks_hit, p.local_blks_read, p.local_blks_dirtied, p.local_blks_written,
    					p.temp_blks_read, p.temp_blks_written,
    					p.blk_read_time, p.blk_write_time
					FROM pg_stat_statements p
					JOIN pg_roles a ON a.oid=p.userid
					JOIN pg_database d ON d.oid=p.dbid
					WHERE p.calls > 100`

	pgStatBasebackupQuery = "SELECT count(pid) AS count, coalesce(extract(epoch from max(now() - backend_start)), 0) AS duration_seconds_max FROM pg_stat_replication WHERE state = 'backup'"

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
    				coalesce(extract(epoch from now() - min((file_stat).access)), 0) AS oldest_file_age_seconds_max
				FROM temp_files GROUP BY 1`

	pgStatWalSizeQuery96 = `SELECT (SELECT count(*) FROM pg_ls_dir('pg_xlog')) * pg_size_bytes(current_setting('wal_segment_size')) as size_bytes`
	pgStatWalSizeQuery   = `SELECT sum(size) AS size_bytes FROM pg_ls_waldir()`

	pgSettingsGucQuery = `SELECT name, unit,
CASE WHEN vartype = 'bool' THEN setting::bool::int::text
	WHEN vartype IN ('string', 'enum') THEN '-1000'::text
	ELSE setting
END AS guc,
CASE WHEN vartype IN ('string', 'enum', 'bool') THEN setting END AS secondary
FROM pg_show_all_settings()`
)
