# pgSCV - PostgreSQL ecosystem metric collector.

---

### pgSCV
- primarily developed for needs of [Weaponry](https://weaponry.io).
- collects a lot of stats about system, PostgreSQL, Pgbouncers, etc
- provides all metrics through well-known /metrics endpoint in [Prometheus metrics exposition format](https://prometheus.io/docs/concepts/data_model/).
- could be configured
    1. in standalone listening-only mode for serving requests from Prometheus.
    2. in combined listening-and-sending mode for sending collected metrics to Weaponry SaaS.
- can run on Linux only, but [can connect](doc/usage-en.md) to remote services running on other OS. 

### Development and contribution
- pgSCV is open-source software.
- All features implemented are focused to Weaponry needs.
- All contributions should be proposed with tests.

### License
BSD-3. See [LICENSE](./LICENSE) for more details.

### Documentation
For further documentation see [usage](doc/usage-en.md).