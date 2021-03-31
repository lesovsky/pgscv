# pgSCV - PostgreSQL ecosystem metric collector.

### pgSCV
- [collects](./doc/collectors.md) a lot of stats about system, PostgreSQL, Pgbouncers, etc.
- provides all metrics through well-known /metrics endpoint in [Prometheus metrics exposition format](https://prometheus.io/docs/concepts/data_model/).
- could be configured:
    1. in standalone listening-only mode for serving requests from Prometheus.
    2. in combined listening-and-sending mode for sending collected metrics to [Weaponry](https://weaponry.io) SaaS.
- can run on Linux only, but [can connect](doc/usage-en.md) to remote services running on other OS.
- include features developed for needs of [Weaponry](https://weaponry.io) users, but might be useless for non-Weaponry users.
- for further info see [full list](doc/features.md) of features.

### Support
If you need help using pgSCV feel free to open discussion or create an [issue](https://github.com/weaponry/pgscv/issues)

### Setup
- [on Ubuntu 20 using deb package](doc/setup-pgscv-deb.md)
- [on CentOS 7 using rpm package](doc/setup-pgscv-rpm.md)
- [on Ubuntu 20 using bootstrap and .tar.gz acrhive](doc/setup-pgscv-targz.md)

### Documentation
For further documentation see [usage](doc/usage-en.md).

### Development and contribution
To help development you are encouraged to:
- provide [suggestion/feedback](https://github.com/weaponry/pgscv/discussions) or [issue](https://github.com/weaponry/pgscv/issues)
- pull requests for new features
- star the project

### License
BSD-3. See [LICENSE](./LICENSE) for more details.
