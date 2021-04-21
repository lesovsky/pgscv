### Installing pgSCV from tar.gz using bootstrap mode on Ubuntu 20.

**IMPORTANT:** This tutorial is intended for using by Weaponry users.

#### TLDR
In this tutorial we are going to configure system and install pgSCV from `.tar.gz` archive using `bootstrap` mode on Ubuntu 20.04.
- create dedicated monitoring roles in Postgres and Pgbouncer (optional)
- install pgSCV from `tar.gz` on Ubuntu 20.04
- configure using `bootstrap` mode
Finally, pgSCV will be run under `root` user with enabled auto-update feature. 

**NOTES**:
- *Bootstrap mode is preferred (but not strict) for Weaponry users, because it requires Weaponry-only settings which are not needed for non-Weaponry users.*

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
   3283 ?        Sl     0:00 /usr/sbin/pgbouncer -d /etc/pgbouncer/pgbouncer.ini
   3184 ?        Ss     0:00 /usr/lib/postgresql/13/bin/postgres -D /var/lib/postgresql/13/main -c config_file=/etc/postgresql/13/main/postgresql.conf
   3186 ?        Ss     0:00  \_ postgres: 13/main: checkpointer 
   3187 ?        Ss     0:00  \_ postgres: 13/main: background writer 
   3188 ?        Ss     0:00  \_ postgres: 13/main: walwriter 
   3189 ?        Ss     0:00  \_ postgres: 13/main: autovacuum launcher 
   3190 ?        Ss     0:00  \_ postgres: 13/main: stats collector 
   3191 ?        Ss     0:00  \_ postgres: 13/main: logical replication launcher 
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
Exact path to `pg_hba.conf` depends on Postgres version. Default path on Ubuntu is version-specific directory inside `/etc/postgresql/`.

After adding lines to `pg_hba.conf`, Postgres service should be reloaded. Connect to Postgres and execute `pg_reload_conf()` function.
```
sudo -u postgres psql -c 'select pg_reload_conf()'
```

Now, test the connection using created database role using `psql` utility. Specify the password in environment variable.
```
PGPASSWORD=SUPERSECRETPASSWORD psql -h 127.0.0.1 -U pgscv -d postgres -c "SELECT version()"
                                                             version                                                              
----------------------------------------------------------------------------------------------------------------------------------
 PostgreSQL 13.2 (Ubuntu 13.2-1.pgdg20.04+1) on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 9.3.0-17ubuntu1~20.04) 9.3.0, 64-bit
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

Download the `tar.gz` archive and extract it using `tar` utility. In this tutorial, v0.4.22 is used, check out the latest version in [releases](https://github.com/weaponry/pgscv/releases) page.
```
wget https://github.com/weaponry/pgscv/releases/download/v0.4.22/pgscv_0.4.22_linux_amd64.tar.gz
tar xvzf pgscv_0.4.22_linux_amd64.tar.gz
```

Specify all necessary environment variables and run pgSCV with `--bootstrap` flag.
**NOTES**:
    - these settings enable auto-update and run service under `root` user. This is necessary because restart service is required during auto-update.
    - if you don't need auto-update or want to use non-root user, specify it with `PGSCV_RUN_AS_USER` and set `PGSCV_AUTOUPDATE=no`.
    - bootstrap mode uses `/usr/local/pgscv` installation path instead of system PATHs.
    - you need to specify your Weaponry project API key (checkout project setup instructions) 
```
sudo -E PGSCV_RUN_AS_USER=root \
PGSCV_SEND_METRICS_URL="https://push.weaponry.io" \
PGSCV_AUTOUPDATE=yes \
PGSCV_API_KEY=00000000-0000-0000-00000-000000000000 \
PGSCV_PG_PASSWORD=SUPERSECRETPASSWORD PGSCV_PGB_PASSWORD=SUPERSECRETPASSWORD \
./pgscv --bootstrap
```

During bootstrap, you should see the following output:
```
{"level":"info","service":"pgscv","time":"2021-03-31T10:29:17+02:00","message":"Running bootstrap"}
{"level":"info","service":"pgscv","time":"2021-03-31T10:29:17+02:00","message":"run pre-flight checks"}
{"level":"info","service":"pgscv","time":"2021-03-31T10:29:17+02:00","message":"Validate bootstrap configuration"}
{"level":"info","service":"pgscv","time":"2021-03-31T10:29:17+02:00","message":"Install agent"}
{"level":"info","service":"pgscv","time":"2021-03-31T10:29:17+02:00","message":"Create config file"}
{"level":"info","service":"pgscv","time":"2021-03-31T10:29:17+02:00","message":"Create systemd unit"}
{"level":"info","service":"pgscv","time":"2021-03-31T10:29:17+02:00","message":"Reload systemd"}
{"level":"info","service":"pgscv","time":"2021-03-31T10:29:17+02:00","message":"bootstrap: waiting until systemd daemon-reload to finish..."}
{"level":"info","service":"pgscv","time":"2021-03-31T10:29:18+02:00","message":"Run agent"}
{"level":"info","service":"pgscv","time":"2021-03-31T10:29:18+02:00","message":"bootstrap: waiting until systemd starts agent service..."}
{"level":"info","service":"pgscv","time":"2021-03-31T10:29:18+02:00","message":"Cleanup"}
{"level":"info","service":"pgscv","time":"2021-03-31T10:29:18+02:00","message":"Bootstrap successful"}
```

This mean, pgscv made the following changes:
- create bindir catalog - `/usr/local/pgscv`
- created config file - `/etc/pgscv.yaml`
- created systemd unit file - `/etc/systemd/system/pgscv.service` and reload systemd daemon
- copy itself into bindir
- enable and start pgscv.service 

Check pgSCV status using `journalctl`. There should be no errors.
```
journalctl -n 40 -fu pgscv
Mar 31 10:29:18 ubuntu-pgscv-test systemd[1]: Started pgSCV is the Weaponry platform agent for PostgreSQL ecosystem.
Mar 31 10:29:18 ubuntu-pgscv-test pgscv[5888]: {"level":"info","service":"pgscv","time":"2021-03-31T10:29:18+02:00","message":"read configuration from /etc/pgscv.yaml"}
Mar 31 10:29:18 ubuntu-pgscv-test pgscv[5888]: {"level":"info","service":"pgscv","time":"2021-03-31T10:29:18+02:00","message":"*** IMPORTANT ***: pgSCV by default collects information about user queries. Tracking queries can be disabled with 'no_track_mode: true' in config file."}
Mar 31 10:29:18 ubuntu-pgscv-test pgscv[5888]: {"level":"info","service":"pgscv","time":"2021-03-31T10:29:18+02:00","message":"no-track mode disabled"}
Mar 31 10:29:18 ubuntu-pgscv-test pgscv[5888]: {"level":"info","service":"pgscv","time":"2021-03-31T10:29:18+02:00","message":"sending metrics to https://push.weaponry.io every 60 seconds"}
Mar 31 10:29:18 ubuntu-pgscv-test pgscv[5888]: {"level":"info","service":"pgscv","time":"2021-03-31T10:29:18+02:00","message":"auto-discovery: service added [system:0]"}
Mar 31 10:29:18 ubuntu-pgscv-test pgscv[5888]: {"level":"info","service":"pgscv","time":"2021-03-31T10:29:18+02:00","message":"start background auto-update loop"}
Mar 31 10:29:18 ubuntu-pgscv-test pgscv[5888]: {"level":"info","service":"pgscv","time":"2021-03-31T10:29:18+02:00","message":"accepting requests on http://127.0.0.1:9890/metrics"}
Mar 31 10:29:18 ubuntu-pgscv-test pgscv[5888]: {"level":"info","service":"pgscv","time":"2021-03-31T10:29:18+02:00","message":"auto-discovery [postgres]: service added [postgres:5432]"}
Mar 31 10:29:18 ubuntu-pgscv-test pgscv[5888]: {"level":"info","service":"pgscv","time":"2021-03-31T10:29:18+02:00","message":"auto-discovery [pgbouncer]: service added [pgbouncer:6432]"}
Mar 31 10:29:18 ubuntu-pgscv-test pgscv[5888]: {"level":"info","service":"pgscv","time":"2021-03-31T10:29:18+02:00","message":"pg_stat_statements is not found in shared_preload_libraries, disable pg_stat_statements metrics collection"}
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