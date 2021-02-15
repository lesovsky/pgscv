## pgSCV usage

---
Index:
- [Features](#features)
- quick start
- requirements
- install
- simple configuration
- extended configuration
  - bootstrap mode
  - configuring collectors (enable/disable)
  - include/exclude filters
  - services
  - weaponry sending metrics
  - ...
- security considerations and audit
- troubleshooting

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

### quick start
Download pgSCV from [releases](https://github.com/weaponry/pgscv/releases).
Unpack the archive.


### requirements

### install

### simple configuration

### extended configuration

#### bootstrap mode

#### configuring collectors (enable/disable)

#### include/exclude filters

#### services

#### weaponry sending metrics

#### ...

### security considerations and audit

### troubleshooting