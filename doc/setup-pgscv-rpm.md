### Installing pgSCV from scratch on CentOS 7.

#### TLDR
In this tutorial we are going to configure system and install pgSCV on CentOS 7 using rpm package.

#### Content:
- [Create database role](#create-database-user)
- [Create pgbouncer user](#create-pgbouncer-user)
- [Install pgSCV](#install-pgscv)
---

### Create database user
Make sure PostgreSQL service should be installed and running. The `ps` command should show running Postgres processes:
```
ps f -u postgres 
  PID TTY      STAT   TIME COMMAND
 1444 ?        Ss     0:00 /usr/pgsql-13/bin/postgres -D /var/lib/pgsql/13/data
 1445 ?        Ss     0:00  \_ postgres: logger 
 1447 ?        Ss     0:00  \_ postgres: checkpointer 
 1448 ?        Ss     0:00  \_ postgres: background writer 
 1449 ?        Ss     0:00  \_ postgres: walwriter 
 1450 ?        Ss     0:00  \_ postgres: autovacuum launcher 
 1451 ?        Ss     0:00  \_ postgres: stats collector 
 1452 ?        Ss     0:00  \_ postgres: logical replication launcher 
```

Connect to Postgres and create database user for pgSCV. This could be unprivileged user with special server roles which allow pgSCV read statistics and traverse directories and files.
```
sudo -u postgres psql
postgres=# CREATE ROLE pgscv WITH LOGIN PASSWORD 'SUPERSECRETPASSWORD';
postgres=# GRANT pg_read_server_files, pg_monitor TO pgscv;
postgres=# GRANT EXECUTE on FUNCTION pg_current_logfile() TO pgscv;
```

Created user should be allowed to connect to Postgres through UNIX sockets and localhost. Add the following lines to `pg_hba.conf`:
```
local   all             pgscv                                   md5
host    all             pgscv           127.0.0.1/32            md5
```
Exact path to `pg_hba.conf` depends on Postgres version. Default path on RHEL-based distros is version-specific directory inside `/var/lib/pgsql`.

After adding lines to `pg_hba.conf`, Postgres service should be reloaded. Connect to Postgres and execute `pg_reload_conf()` function.
```
sudo -u postgres psql
postgres=# select pg_reload_conf();
```

Now, test the connection using created database role using `psql` utility. Specify the password in environment variable.
```
PGPASSWORD=SUPERSECRETPASSWORD psql -h 127.0.0.1 -U pgscv -d postgres -c "SELECT version()"
                                                 version                                                 
---------------------------------------------------------------------------------------------------------
 PostgreSQL 13.2 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 4.8.5 20150623 (Red Hat 4.8.5-44), 64-bit
```
In this example we connect to Postgres and ask its version.

### Create pgbouncer user
In case of using Pgbouncer, additional configuration have to be made. Add `pgscv` user to `stats_users` list in `pgbouncer.ini`
```
stats_users = pgscv
```

After changing `pgbouncer.ini`, Pgbouncer service should be reloaded.
```
systemctl reload pgbouncer
```

Depending on used `auth_type` user and password should be specified in `auth_file`. By default, it is `userlist.txt`. For example for `auth_type = md5`, user and password could be added in the following way:
```
echo -n "SUPERSECRETPASSWORD" |md5sum |awk -v user=pgscv '{printf "\"%s\" \"md5%s\"\n", user, $1}' >> /etc/pgbouncer/userlist.txt
```

Now, make test connection to Pgbouncer. Specify the password in environment variable.
```
PGPASSWORD=SUPERSECRETPASSWORD psql -h 127.0.0.1 -p 6432 -U pgscv -d pgbouncer -c "SHOW version"
     version      
------------------
 PgBouncer 1.15.0
```
In this example we connect to Pgbouncer built-in database and ask its version.

### Install pgSCV

Download and install the `rpm` package using `yum` utility. In this tutorial, v0.4.17 is used, check out the latest version in [releases](https://github.com/weaponry/pgscv/releases) page.
```
yum install https://github.com/weaponry/pgscv/releases/download/v0.4.17/pgscv_0.4.17_linux_amd64.rpm
```

Create pgSCV default configuration in `/etc/pgscv.yaml` with the credentials created in previous steps.
```
defaults: 
    postgres_username: "pgscv"
    postgres_password: "SUPERSECRETPASSWORD"
    pgbouncer_username: "pgscv"
    pgbouncer_password: "SUPERSECRETPASSWORD"
```

Create a unit file for systemd service `/etc/systemd/system/pgscv.service` with the following content:
```
[Unit]
Description=pgSCV is the Weaponry platform agent for PostgreSQL ecosystem
Requires=network-online.target
After=network-online.target

[Service]
Type=simple
User=postgres
Group=postgres

# Start the agent process
ExecStart=/usr/bin/pgscv --config-file=/etc/pgscv.yaml

# Only kill the agent process
KillMode=control-group

# Wait reasonable amount of time for agent up/down
TimeoutSec=5

# Restart agent if it crashes
Restart=on-failure
RestartSec=10

# if agent leaks during long period of time, let him to be the first person for eviction
OOMScoreAdjust=1000

[Install]
WantedBy=multi-user.target
```

Reload systemd and start pgSCV service.
```
systemctl daemon-reload
systemctl enable pgscv
systemctl start pgscv
```

Check pgSCV status using `journalctl`. There should be no errors.
```
journalctl -fu pgscv
мар 31 09:47:05 centos-pgscv-test systemd[1]: Started pgSCV is the Weaponry platform agent for PostgreSQL ecosystem.
мар 31 09:47:05 centos-pgscv-test pgscv[1558]: {"level":"info","service":"pgscv","time":"2021-03-31T09:47:05+02:00","message":"read configuration from /etc/pgscv.yaml"}
мар 31 09:47:05 centos-pgscv-test pgscv[1558]: {"level":"info","service":"pgscv","time":"2021-03-31T09:47:05+02:00","message":"*** IMPORTANT ***: pgSCV by default collects information about user queries. Tracking queries can be disabled with 'no_track_mode: true' in config file."}
мар 31 09:47:05 centos-pgscv-test pgscv[1558]: {"level":"info","service":"pgscv","time":"2021-03-31T09:47:05+02:00","message":"no-track mode disabled"}
мар 31 09:47:05 centos-pgscv-test pgscv[1558]: {"level":"info","service":"pgscv","time":"2021-03-31T09:47:05+02:00","message":"accepting requests on http://127.0.0.1:9890/metrics"}
мар 31 09:47:05 centos-pgscv-test pgscv[1558]: {"level":"info","service":"pgscv","time":"2021-03-31T09:47:05+02:00","message":"auto-discovery: service added [system:0]"}
мар 31 09:47:05 centos-pgscv-test pgscv[1558]: {"level":"info","service":"pgscv","time":"2021-03-31T09:47:05+02:00","message":"auto-discovery [postgres]: service added [postgres:5432]"}
мар 31 09:47:05 centos-pgscv-test pgscv[1558]: {"level":"info","service":"pgscv","time":"2021-03-31T09:47:05+02:00","message":"auto-discovery [pgbouncer]: service added [pgbouncer:6432]"}
мар 31 09:47:05 centos-pgscv-test pgscv[1558]: {"level":"info","service":"pgscv","time":"2021-03-31T09:47:05+02:00","message":"pg_stat_statements is not found in shared_preload_libraries, disable pg_stat_statements metrics collection"}
```

Connect to pgSCV using `curl` and ask metrics, there should be non-zero counts.
```
curl -s http://127.0.0.1:9890/metrics | grep -c ^postgres
411
curl -s http://127.0.0.1:9890/metrics | grep -c ^pgbouncer
100
curl -s http://127.0.0.1:9890/metrics | grep -c ^node
288
curl -s http://127.0.0.1:9890/metrics | grep -c ^go
34
```

In case of errors, see [troubleshooting](./usage-en.md#troubleshooting) notes