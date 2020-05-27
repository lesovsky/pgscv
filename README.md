# pgscv v2
pgSCV is the agent tool that gathers activity stats from PostgreSQL and PostgreSQL-related services and pushes stats to Prometheus pushgateway.

#### Description
pgSCV is the agent utility for metric collecting.

#### Features
- PULL mode - pgSCV listen on `/metric` endpoint for incoming requests
- PUSH mode - pgSCV collects metrics and send it to specified URL
- local services auto-discovery (root privileges required)
- collect metrics from user-defined services
- run as different system user
- auto-upgrade procedure (root privileges required)
- bootstrap procedure (root privileges required)
- support collecting metrics from:
  - system
  - postgres
  - pgbouncer

#### Getting started
1. Setup database and pgbouncer users (CREATE ROLE, add HBA rules)
2. Download pgSCV
3. Set bootstrap environments
4. Run pgSCV in bootstrap mode

#### Custom configuration
- use user-defined services
- select between PULL and PUSH modes
- override connections defaults
