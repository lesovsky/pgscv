## Collectors

List of available collectors and information collected by them 

---

Index of content:
- [System collectors](#system-collectors)
- [PostgreSQL collectors](#postgresql-collectors)
- [Pgbouncer collectors](#pgbouncer-collectors)
- [Miscellaneous collectors](#miscellaneous-collectors)

### System collectors
- system/loadaverage: load averages from `/proc/loadavg`
- system/cpu: cpu usage stats from `/proc/stat`
- system/diskstats: block devices stats from `/proc/diskstats`
- system/filesystems: filesystem stats from `/proc/mounts`
- system/netdev: network interfaces stats from `/proc/net/dev`
- system/network: network settings information
- system/memory: memory stats from `/proc/meminfo`, `/proc/vmstat`
- system/sysconfig: system config from `/proc/sys`, `/sys/devices/system`

### PostgreSQL collectors
- postgres/activity: activity stats from `pg_stat_activity`
- postgres/archiver: WAL archiver stats from `pg_stat_archiver`; **required Postgres 12 or newer**
- postgres/bgwriter: background writer and checkpointer stats from `pg_stat_bgwriter`
- postgres/conflicts: recovery conflicts during replication, from `pg_stat_database_conflicts`
- postgres/databases: databases stats from `pg_stat_databases`
- postgres/indexes: indexes stats from `pg_stat_user_indexes`, `pg_statio_user_indexes`
- postgres/functions: functions stats from `pg_stat_user_functions`
- postgres/locks: activity locks from `pg_locks`
- postgres/logs: log messages from Postgres log files; **required Postgres 10 or newer**
- postgres/replication: replication stats from `pg_stat_replication`
- postgres/replication_slots: stats about replication slots from `pg_replication_slots`
- postgres/statements: statements stats from `pg_stat_statements`
- postgres/schemas: databases' schemas stats from system catalog; **required Postgres 9.5 or newer**
- postgres/settings: Postgres settings based on `pg_show_all_settings()`
- postgres/storage: data files/directories stats; **required Postgres 10 or newer**
- postgres/tables: tables stats from `pg_stat_user_tables`, `pg_statio_user_tables`

### Pgbouncer collectors
- pgbouncer/pools: stats based on `SHOW POOLS` command
- pgbouncer/stats: stats based on `SHOW STATS` command
- pgbouncer/settings: settings from `SHOW CONFIG` command

### Miscellaneous collectors
- system/pgscv: pgSCV internal metrics