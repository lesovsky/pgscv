## Features

List of available features. List of available collectors available [here](./collectors.md)

---

- Collecting metrics:
  - System stats
  - PostgreSQL stats, logs
  - Pgbouncer stats


- Selective disabling of specific collectors. Users might disable specific collectors if some metrics are not needed.


- Expose metrics in [Prometheus metrics exposition format](https://prometheus.io/docs/concepts/data_model/). Metrics are
  available through standard HTTP `/metrics` endpoint.


- Services auto-discovery. pgSCV can discover services running on the same host where pgSCV is running and automatically 
  start collecting metrics.
  - PostgreSQL services
  - Pgbouncer services


- Remote service connecting. pgSCV can connect to remote services using user-provided requisites.


- Auto-update. pgSCV can track Github Releases page for new releases and automatically update itself. Mainly this feature
  exists for [Weaponry](https://weaponry.io) users for automatic pgSCV updates and allow delivering new Weaponry features
  quickly.   