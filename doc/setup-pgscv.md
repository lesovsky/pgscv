# Setup pgSCV

This instruction describes the full process of pgSCV setup.

Index of content:
- [Configure Postgres](#configure-postgres)
- [Configure Pgbouncer](#configure-pgbouncer)
- [Setup pgSCV](#setup-pgscv)
- [Post-setup checks](#post-setup-checks)

---

### Configure Postgres

pgSCV requires database user for connecting to Postgres and collecting metrics. This database user should have necessary 
privileges to collect stats.

The built-in database `pg_monitor` and `pg_read_server_files` roles could be used, but unfortunately it is not completely 
enough and extra privileges should be granted.

**Note**: `pg_monitor` and `pg_read_server_files` roles added in Postgres 10. 

Connect to Postgres and create a role with the following command:
```
CREATE ROLE pgscv WITH LOGIN PASSWORD 'supersecretpassword';
GRANT pg_read_server_files, pg_monitor TO pgscv;
```

The `EXECUTE` privilege also required for executing `pg_current_logfile()` function - its execution is not granted for `pg_monitor`.
Connect to `postgres` database and execute the following command:
```
GRANT EXECUTE on FUNCTION pg_current_logfile() TO pgscv;
```

For connecting to Postgres under new role a new hba rules might be required. Add the following line into pg_hba.conf file:
```
host all pgscv 127.0.0.1/32 md5
```

After editing pg_hba.conf Postgres service should be reloaded - connect to Postgres and reload the service:
```
SELECT pg_reload_conf();
```

Check Postgres log that changes have been applied with no errors.

Before continue, make sure you are able to connect to Postgres using the created user:
```
psql -h 127.0.0.1 -U pgscv -d postgres
```

### Configure Pgbouncer

pgSCV requires pgbouncer user for connecting to Pgbouncer and collecting metrics. 

Pgbouncer provides special `stats_users` parameter for monitoring users. Edit the `pgbouncer.ini`, add the user to the 
`stats_users` list. After edit config file, Pgbouncer service have to be reloaded.

Next, depending on used `auth_type` you need to allow connecting the user to Pgbouncer. The most wide-used auth type
is `md5`, let's consider setup steps for this auth method.

**Note**: The next step is considered only for **auth_type = md5**.

Export username and password as environment variables and generate md5 hash: 
```
export PGB_USERNAME=pgscv PGB_PASSWORD=supersecretpassword
echo -n "$PGB_PASSWORD$PGB_USERNAME" |md5sum |awk -v user=$PGB_USERNAME '{printf "\"%s\" \"md5%s\"\n", user, $1}'
unset PGB_USERNAME PGB_PASSWORD
```

The result string with the user's name and hash (something like a `"pgscv" "md5...."`) should be added in the end of file
specified in `auth_file` setting.

After string has been added Pgbouncer will automatically re-read auth file and apply changes. No need to reload Pgbouncer
service explicitly.

Before continue, make sure you are able to connect to Pgbouncer using the created user (exact address and port are depend
on `listen_addr` and `listen_port` parameter):
```
psql -h 127.0.0.1 -p 6432 -U pgscv -d pgbouncer
```

### Setup Pgscv

Download the latest release of pgSCV from releases page, unpack it, and copy to system path.
```
wget https://github.com/weaponry/pgscv/releases/...
tar xvzf pgscv.tar.gz
cp pgscv /usr/bin/
```

Create YAML configuration file, for example `/etc/pgscv.yaml` with the following content:
```
defaults: 
    postgres_username: "pgscv"
    postgres_password: "supersecretpasssword"
    pgbouncer_username: "pgscv"
    pgbouncer_password: "supersecretpassword"
```

This YAML configuration contains auth information, set secure permissions on this file, e.g. `0600`.

For more YAML configuration options see [usage](./usage-en.md#yaml-configuration-settings)

Create systemd unit file - `/etc/systemd/system/pgscv.service` with the following content:
```
[Unit]
Description=pgSCV is the Weaponry platform agent for PostgreSQL ecosystem
After=network.target

[Service]
Type=simple

User=postgres
Group=postgres

# Start the agent process
ExecStart=/usr/bin/pgscv --config-file=/etc/pgscv.yaml

# Only kill the agent process
KillMode=process

# Wait reasonable amount of time for agent up/down
TimeoutSec=5

# Restart agent if it crashes
Restart=on-failure

# if agent leaks during long period of time, let him to be the first person for eviction
OOMScoreAdjust=1000

[Install]
WantedBy=multi-user.target
```

Reload systemd configuration, enable and start `pgscv` service:
```
systemctl daemon-reload
systemctl enable pgscv
systemctl start pgscv
```

### Post-setup checks

Check `pgscv` service has been started - service should be in **active (running)** state:
```
systemctl status pgscv
● pgscv.service - pgSCV is the Weaponry platform agent for PostgreSQL ecosystem
     Loaded: loaded (/etc/systemd/system/pgscv.service; disabled; vendor preset: enabled)
     Active: active (running) since Mon 2021-02-15 21:58:25 +05; 12h ago
   Main PID: 2469573 (pgscv)
      Tasks: 17 (limit: 38375)
     Memory: 36.0M
     CGroup: /system.slice/pgscv.service
             └─2469573 /usr/bin/pgscv --config-file=/etc/pgscv.yaml
```

In case of errors, see [troubleshooting](./usage-en.md#troubleshooting) notes