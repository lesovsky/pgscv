# pgSCV - PostgreSQL ecosystem metrics collector.

### pgSCV
- [collects](https://github.com/lesovsky/pgscv/wiki/Collectors) a lot of stats about PostgreSQL environment.
- exposes metrics through the HTTP `/metrics` endpoint in [Prometheus metrics exposition format](https://prometheus.io/docs/concepts/data_model/).

**IMPORTANT NOTES**
1. pgSCV is archived and is not maintained. Check out the another fork [CHERTS/pgscv](https://github.com/CHERTS/pgscv). 
2. pgSCV moved from 'weaponry' to 'lesovsky' GitHub account. From version 0.8.0 all features required for Weaponry will be removed:
- auto-discovery (all Postgres, Pgbouncer services have to be defined explicitly, by configuration file or environment variables)
- bootstrap, uninstall and auto-update
- push metrics to remote service
- Patroni support (because it has this feature built-in)

### Features
- **Supported services:** support collecting metrics of PostgreSQL and Pgbouncer.
- **OS metrics:** support collecting metrics of operating system.
- **TLS and authentication**. `/metrics` endpoint could be protected with basic authentication and TLS.
- **Collecting metrics from multiple services**. pgSCV can collect metrics from many databases instances.
- **User-defined metrics**. pgSCV could be configured in a way to collect metrics defined by user.
- **Collectors management**. Collectors could be disabled if necessary.
- **Collectors filters**. Collectors could be adjusted to skip collecting metrics based on labels values, like
  block devices, network interfaces, filesystems, users, databases, etc.

### Requirements
- can run on Linux only; can connect to remote services running on other OS/PaaS.
- requisites for connecting to the services, such as login and password.
- database user should have privileges for executing stats functions and reading views.
  For more details see [security considerations](https://github.com/lesovsky/pgscv/wiki/Security-considerations).

### Quick start
Download the archive from [releases](https://github.com/lesovsky/pgscv/releases). Unpack the archive. Start pgSCV under `postgres` user.

```shell
wget https://github.com/lesovsky/pgscv/releases/download/v0.7.5/pgscv_0.7.5_linux_amd64.tar.gz
tar xvzf pgscv_0.7.5_linux_amd64.tar.gz
sudo -u postgres ./pgscv 
```

or using Docker, use `DATABASE_DSN` for setting up a connection to Postgres:
```
docker pull lesovsky/pgscv:latest
docker run -ti -e PGSCV_LISTEN_ADDRESS=0.0.0.0:9890 -e PGSCV_DISABLE_COLLECTORS="system" -e DATABASE_DSN="postgresql://postgres@dbhost/postgres" -p 9890:9890 lesovsky/pgscv:latest
```

When pgSCV has been started it is ready to accept HTTP requests at `http://127.0.0.1:9890/metrics`.

### Complete setup
Checkout complete setup [guide](https://github.com/lesovsky/pgscv/wiki/Setup-for-regular-users).

### Documentation
For further documentation see [wiki](https://github.com/lesovsky/pgscv/wiki).

### Support and feedback
If you need help using pgSCV feel free to open discussion or create an [issue](https://github.com/lesovsky/pgscv/issues)

### Development and contribution
To help development you are encouraged to:
- provide [suggestion/feedback](https://github.com/lesovsky/pgscv/discussions) or [issue](https://github.com/lesovsky/pgscv/issues)
- pull requests for new features
- star the project

### Authors
- [Lesovsky Alexey](https://github.com/lesovsky)

### License
BSD-3. See [LICENSE](./LICENSE) for more details.
