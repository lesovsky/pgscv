# pgSCV - PostgreSQL ecosystem metrics collector.

### pgSCV
- [collects](https://github.com/weaponry/pgscv/wiki/Collectors) a lot of stats about system, PostgreSQL, Pgbouncers, etc.
- exposes metrics through the HTTP `/metrics` endpoint in [Prometheus metrics exposition format](https://prometheus.io/docs/concepts/data_model/).

### Features
- **Pull mode**. pgSCV can listen on `/metrics` endpoint and serving requests from `Prometheus` or `Victoriametrics' Vmagent`.
- **Push mode**. pgSCV can scrape its own `/metrics` endpoint and push scraped metrics to specified HTTP service.
  This feature primarily used for sending metrics to Weaponry SaaS, but not limited by this purpose.
- **Services auto-discovery**. pgSCV can automatically discover Postgres and other Postgres-ecosystem services and
  start collecting metrics from them. In case of authentication, valid requisites should be specified.
- **Remote services support**. pgSCV is recommended to start on the same systems where monitored services are running.
  But this is not strict and pgSCV could connect and collect metrics from remote services.
- **Bootstrap**. pgSCV can bootstrap itself - it is one-time procedure, during bootstrap pgSCV installs itself into system path, creates minimal required configuration,
  installs systemd unit and starts itself. **Requires root privileges.**
- **Auto-update**. pgSCV can track new releases and update itself. This feature is mostly useful for Weaponry users. **Requires root privileges.**
- **User-defined metrics**. pgSCV could be configured in a way to collect metrics defined by user.
- **Collectors management**. Collectors could be disabled if necessary.
- **Collectors filters**. Some collectors could be adjusted to skip collecting metrics about unnecessary stuff, like
  block devices, network interfaces, filesystems, etc.

### Requirements
- can run on Linux only; can connect to remote services running on other OS/PaaS.
- requisites for connecting to the services, such as login and password.
- database user should have privileges for executing stats functions and reading views.
  For more details see [security considerations](https://github.com/weaponry/pgscv/wiki/Security-considerations).

### Quick start
Download the archive from [releases](https://github.com/weaponry/pgscv/releases). Unpack the archive. Start pgSCV under `postgres` user.

```shell
wget https://github.com/weaponry/pgscv/releases/download/v0.5.0/pgscv_0.5.0_linux_amd64.tar.gz
tar xvzf pgscv_0.5.0_linux_amd64.tar.gz
sudo -u postgres ./pgscv 
```

When pgSCV has been started it is ready to accept HTTP requests at `http://127.0.0.1:9890/metrics`.

### Complete setup
pgSCV complete setup is possible in two ways:
1. For **non-Weaponry** users. Setup as a standalone service, which accepts metrics scrape requests only. Checkout complete setup [guide](https://github.com/weaponry/pgscv/wiki/Setup-for-regular-users).
2. For **Weaponry** users. Setup as an agent of Weaponry SaaS, which receives metrics to Weaponry service. Checkout complete setup [guide](https://github.com/weaponry/pgscv/wiki/Setup-for-Weaponry-users). Listening for scrape requests also work.

### Documentation
For further documentation see [wiki](https://github.com/weaponry/pgscv/wiki).

### Support and feedback
If you need help using pgSCV feel free to open discussion or create an [issue](https://github.com/weaponry/pgscv/issues)

### Development and contribution
To help development you are encouraged to:
- provide [suggestion/feedback](https://github.com/weaponry/pgscv/discussions) or [issue](https://github.com/weaponry/pgscv/issues)
- pull requests for new features
- star the project

### Authors
- [Lesovsky Alexey](https://github.com/lesovsky)

### License
BSD-3. See [LICENSE](./LICENSE) for more details.
