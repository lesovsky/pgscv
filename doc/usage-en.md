## pgSCV usage

---
Index of content:
- [Features](#features)
- [Requirements](#requirements)
- [Quick start](#quick-start)
- [YAML configuration](#yaml-configuration-settings)
- [Bootstrap and Uninstall modes](#bootstrap-and-uninstall-modes)
- [Security considerations](#security-considerations)
- [Troubleshooting](#troubleshooting)

---

### Features
- **Pull mode**. pgSCV can listen on `/metrics` endpoint and serving requests from `Prometheus` or `Victoriametrics' Vmagent`.
- **Push mode**. pgSCV can scrape its own `/metrics` endpoint and push scraped metrics to specified HTTP service.
  This feature primarily used for sending metrics to Weaponry SaaS, but not limited by this purpose.
- **Services auto-discovery**. pgSCV can automatically discover Postgres and other Postgres-ecosystem services and
  start collecting metrics from them. In case of authentication, valid requisites should be specified.
- **Remote services support**. pgSCV is recommended to start on the same systems where monitored services are running.
  But this is not strict and pgSCV could connect and collect metrics from remote services. 
- **Bootstrap**. pgSCV can bootstrap itself - install itself to system path, create minimal required configuration, 
  install systemd unit and start itself.
- **Collectors management**. Collectors could be disabled if necessary.
- **Collectors filters**. Some collectors could be adjusted to skip collecting metrics about unnecessary stuff, like 
  block devices, network interfaces, filesystems, etc.

### Requirements
- requisites for connecting to the services, such as login and password.
- database user should have privileges for executing stats functions and reading views.
For more details see security considerations.

### Quick start
Download the archive from [releases](https://github.com/weaponry/pgscv/releases). Unpack the archive. Start pgscv under `postgres` user.

```shell
$ wget ...
$ tar xvzf pgscv.tar.gz
$ sudo -u postgres ./pgscv 
```

### YAML Configuration settings
pgSCV configuration settings are defined in YAML configuration file. Location of the configuration file could be specified 
at startup using `--config-file` option. pgSCV can run without configuration file, in this case default values will be
used.

- **listen_address**: network address and port where the application should listen on. Default value: `127.0.0.1:10090`.


- **autoupdate_url**: URL used for tracking pgSCV updates. Default value: "".


- **no_track_mode**: controls tracking of sensitive information, such as query texts. Default value: false (disabled).


- **send_metrics_url**: URL of the remote service where collected metrics should be sent. Default value: "" (disabled).


- **api_key**: API key for accessing to Weaponry service. Default value: "". *Needed only for Weaponry clients.*


- **project_id**: Project ID of the Weaponry project which host is associated. Default: 0 (disabled). *Needed only for Weaponry clients.*


- **services**: list of services to which pgSCV should connect and monitor. Defining `services` automatically disables 
auto-discovery. Empty by default, looking for services using auto-discovery.
  - **service_type**: type of the service, must be one of `postgres`, `pgbouncer`.
  - **conninfo**: connection string or DSN for connecting to service.


- **defaults**: default requisites for connecting to auto-discovered services. 
  - **postgres_dbname**: database name for connecting to services which are identified as Postgres. Default value: "postgres".
  - **postgres_username**: username for connecting to services which are identified as Postgres. Default value: "pgscv".
  - **postgres_password**: password for connecting to services which are identified as Postgres. Default value: "".
  - **pgbouncer_dbname**: database name for connecting to services which are identified as Pgbouncer. Default value: "pgbouncer".
  - **pgbouncer_username**: username for connecting to services which are identified as Pgbouncer. Default value: "pgscv".
  - **pgbouncer_password**: password for connecting to services which are identified as Pgbouncer. Default value: "".


- **filters**: per-collector filtering rules for including or excluding specific collector objects. Exclude rules has
  higher priority over include rules.
  - **collector_name/label_name**: exact name of [collector](./collectors.md).
    - **include**: regexp string for including objects.
    - **exclude**: regexp string for excluding objects. Has higher priority over `include`.
  
  Supported filters. Currently, only the following list of filters are available:
  - **diskstats/device**: exclude: `^(ram|loop|fd|sr|(h|s|v|xv)d[a-z]|nvme\d+n\d+p)\d+$`
  - **netdev/device**: exclude: `docker|virbr`
  - **filesystem/fstype**: include: `^(ext3|ext4|xfs|btrfs)$`


- **disable_collectors**: list of [collectors](./collectors.md) which should be disabled. Default value: [] (all collectors are enabled).

YAML configuration file example:
```
listen_address: 127.0.0.1:10090
autoupdate_url: https://github.com/weaponry/pgscv/releases
no_track_mode: false
send_metrics_url: https://push.weaponry.io
api_key: 12345678-abcd-1234-abcd-123456789012
project_id: 12345678
services:
  - service_type: "postgres"
    conninfo: "postgres://postgres@127.0.0.1:5432/postgres"
  - service_type: "pgbouncer"
    conninfo: "postgres://pgbouncer@127.0.0.1:6432/pgbouncer"
defaults:
    postgres_username: "monitoring"
    postgres_password: "supersecret"
    pgbouncer_username: "monitoring"
    pgbouncer_password: "supersecret"
filters:
    - diskstats/device:
      exclude: "^(ram|loop|fd|sr|(h|s|v|xv)d[a-z]|nvme\d+n\d+p)\d+$"
    - netdev/device:
      exclude: "docker|virbr"
    - filesystem/fstype:
      include: "^(ext3|ext4|xfs|btrfs)$"
```

### Bootstrap and Uninstall modes
pgSCV provides bootstrap and uninstall modes for quick install or (uninstall). During the bootstrap pgSCV does:
- copy itself to system PATH catalog
- create YAML configuration
- create `pgscv` systemd service
- enable and start `pgscv` systemd service

Uninstall mode revert all changes:
- stop and disable `pgscv` systemd service
- removes `pgscv` systemd service
- remove YAML configuration
- remove itself from system PATH

Executing bootstrap or uninstall requires root privileges or sudo for accessing to system paths.

For configuring YAML configuration during bootstrap, the following environment variables are available:
- PGSCV_RUN_AS_USER - which user should be used for running `pgscv` systemd service
- PGSCV_SEND_METRICS_URL - value for **send_metrics_url** YAML setting
- PGSCV_AUTOUPDATE_URL - value for **autoupdate_url** YAML setting
- PGSCV_API_KEY - value for **api_key** YAML setting
- PGSCV_PROJECT_ID - value for **project_id** YAML setting
- PGSCV_PG_PASSWORD - value for **defaults.postgres_password** YAML setting
- PGSCV_PGB_PASSWORD - value for **defaults.pgbouncer_password** YAML setting

### Security considerations
For collecting metrics and auto-discovery pgSCV requires some kind of privileges. pgSCV uses the following sources for collecting metrics:
- Procfs and Sysfs pseudo-filesystems
- Postgres and Pgbouncer log files
- Postgres stat views beginning from `pg_stat` prefix
- Postgres system catalog tables
- Postgres admin functions
- Filesystem paths in Postgres data directory (for auto-discovery)
- Filesystem paths in /etc (for auto-discovery)
- Pgbouncer's stats from `pgbouncer` built-in database.

**System access**
- regular, unprivileged system user is sufficient to read all necessary stats.
- this user must have access to Postgres/Pgbouncer log directories
- when **autoupdate_url** is enabled, pgSCV requires write-access to the directory where `pgscv`
binary is stored.

**Postgres access**
- regular, unprivileged database role is *NOT* sufficient to read all necessary stats
- at least `pg_monitor` and `pg_read_server_files` roles must be granted to the role (available since Postgres 10)
- an `EXECUTE` privilege must be granted on `pg_current_logfile()` function in database used for connecting (default is
  `postgres`)

**Pgbouncer access**
- user specified in `stats_users` of `pgbouncer.ini` is sufficient to read all necessary stats.

### Troubleshooting
- Check pgSCV is running by systemd - service should be in **active (running)** state:
```
# systemctl status pgscv
● pgscv.service - pgSCV is the Weaponry platform agent for PostgreSQL ecosystem
     Loaded: loaded (/etc/systemd/system/pgscv.service; disabled; vendor preset: enabled)
     Active: active (running) since Mon 2021-02-15 21:58:25 +05; 12h ago
   Main PID: 2469573 (pgscv)
      Tasks: 17 (limit: 38375)
     Memory: 36.0M
     CGroup: /system.slice/pgscv.service
             └─2469573 /usr/bin/pgscv --config-file=/etc/pgscv.yaml
```

- Check pgSCV exists in process list
```
# ps auxf |grep pgscv |grep -v grep
postgres 2469573  1.3  0.1 721044 41524 ?        Ssl  feb15  10:26 /usr/bin/pgscv --config-file=/etc/pgscv.yaml
```

- Check port is opened by pgSCV - port specified in YAML configuration should be opened (or default one)
```
# ss -luntp|grep pgscv
tcp    LISTEN  0       4096              127.0.0.1:10090          0.0.0.0:*      users:(("pgscv",pid=2469573,fd=7))
```

- Try to request `/metrics` HTTP endpoint - 200 OK should be returned.
```
# curl -I http://127.0.0.1:10090/metrics
HTTP/1.1 200 OK
Content-Type: text/plain; version=0.0.4; charset=utf-8
Date: Tue, 16 Feb 2021 05:45:22 GMT
```

- Try to request particular metrics, count or filter using `grep`.
```
# curl -s http://127.0.0.1:10090/metrics |grep -c '^postgres_'
7386
```

- Check log messages using `journalctl`
```
# journalctl -fu pgscv
-- Logs begin at Wed 2020-10-28 08:30:51 +05. --
feb 15 21:58:25 matanuii pgscv[2469573]: {"level":"info","service":"pgscv","time":"2021-02-15T21:58:25+05:00","message":"auto-discovery [pgbouncer]: service added [pgbouncer:6432]"}
feb 15 21:58:25 matanuii pgscv[2469573]: {"level":"info","service":"pgscv","time":"2021-02-15T21:58:25+05:00","message":"auto-discovery [postgres]: service added [postgres:5432]"}
feb 15 21:58:26 matanuii pgscv[2469573]: {"level":"info","service":"pgscv","time":"2021-02-15T21:58:26+05:00","message":"starting tail of /var/log/postgresql/postgresql-replica-Mon.log from the end"}
feb 15 21:58:26 matanuii pgscv[2469573]: {"level":"info","service":"pgscv","time":"2021-02-15T21:58:26+05:00","message":"starting tail of /var/log/postgresql/postgresql-Mon.log from the end"}
feb 16 00:00:01 matanuii pgscv[2469573]: 2021/02/16 00:00:01 Re-opening truncated file /var/log/postgresql/postgresql-Mon.log ...
feb 16 00:00:01 matanuii pgscv[2469573]: 2021/02/16 00:00:01 Successfully reopened truncated /var/log/postgresql/postgresql-Mon.log
feb 16 00:00:26 matanuii pgscv[2469573]: {"level":"info","service":"pgscv","time":"2021-02-16T00:00:26+05:00","message":"logfile changed, stopping current tailing"}
feb 16 00:00:26 matanuii pgscv[2469573]: {"level":"info","service":"pgscv","time":"2021-02-16T00:00:26+05:00","message":"starting tail of /var/log/postgresql/postgresql-replica-Tue.log from the beginning"}
feb 16 00:00:26 matanuii pgscv[2469573]: {"level":"info","service":"pgscv","time":"2021-02-16T00:00:26+05:00","message":"logfile changed, stopping current tailing"}
feb 16 00:00:26 matanuii pgscv[2469573]: {"level":"info","service":"pgscv","time":"2021-02-16T00:00:26+05:00","message":"starting tail of /var/log/postgresql/postgresql-Tue.log from the beginning"}
```

- pgSCV has `--log-level` option, supported values are: debug, info, warn, error. Default value: info.