## Collectors

List of available collectors and information collected by them 

---

Index of content:
- [System collectors](#system-collectors)
- [PostgreSQL collectors](#postgresql-collectors)
- [Pgbouncer collectors](#pgbouncer-collectors)
- [Miscellaneous collectors](#miscellaneous-collectors)

### System collectors

| Collector | Datasource | Description |
| --- | --- | --- |
| system/loadaverage | `/proc/loadavg` | load averages
| system/cpu | `/proc/stat` | cpu usage
| system/diskstats | `/proc/diskstats` | block devices usage
| system/filesystems | `/proc/mounts` | filesystem usage
| system/netdev | `/proc/net/dev` | network interfaces usage
| system/network |  | network settings information
| system/memory | `/proc/meminfo`, `/proc/vmstat` | memory usage
| system/sysconfig | `/proc/sys`, `/sys/devices/system` | system config

### PostgreSQL collectors
| Collector | Datasource | Description |
| --- | --- | --- |
| postgres/activity | `pg_stat_activity` | current activity stats
| postgres/archiver | `pg_stat_archiver` | WAL archiving stats; **required Postgres 12 or newer**
| postgres/bgwriter | `pg_stat_bgwriter` | background writer and checkpointer stats
| postgres/conflicts | `pg_stat_database_conflicts` | recovery conflicts occurred during replication
| postgres/databases | `pg_stat_databases` | databases general stats
| postgres/indexes | `pg_stat_user_indexes`, `pg_statio_user_indexes` | indexes usage and IO stats
| postgres/functions | `pg_stat_user_functions` | functions usage and timings stats
| postgres/locks | `pg_locks` | current activity locks
| postgres/logs |  | Postgres log messages; **required Postgres 10 or newer**
| postgres/replication | `pg_stat_replication` | replication stats
| postgres/replication_slots | `pg_replication_slots` | replication slots usage stats
| postgres/statements | `pg_stat_statements` | executed statements stats
| postgres/schemas |  | databases' schemas stats from system catalog; **required Postgres 9.5 or newer**
| postgres/settings | `pg_show_all_settings()` | current settings
| postgres/storage |  | data files/directories usage stats; **required Postgres 10 or newer**
| postgres/tables | `pg_stat_user_tables`, `pg_statio_user_tables` | tables usage and IO stats

### Pgbouncer collectors
| Collector | Datasource | Description |
| --- | --- | --- |
| pgbouncer/pools | `SHOW POOLS` | pools usage stats
| pgbouncer/stats | `SHOW STATS` | general pgbouncer stats
| pgbouncer/settings | `SHOW CONFIG` | current settings (including per-database settings)

### Miscellaneous collectors
| Collector | Datasource | Description |
| --- | --- | --- |
| system/pgscv | internal | pgSCV internal metrics 