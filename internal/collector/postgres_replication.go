package collector

import (
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
)

const (
	// Query for Postgres version 9.6 and older.
	postgresReplicationQuery96 = `SELECT
    pid, coalesce(client_addr, '127.0.0.1') AS client_addr, usename, application_name, state,
    pg_is_in_recovery()::int AS recovery,
		(case pg_is_in_recovery() when 't' then null else pg_current_xlog_location() - '0/00000000' end) AS wal_bytes,
		pg_current_xlog_location() - sent_lsn AS pending_lag_bytes,
		sent_location - write_location AS write_lag_bytes,
		write_location - flush_location AS flush_lag_bytes,
		flush_location - replay_location AS replay_lag_bytes,
		pg_current_xlog_location() - replay_location AS total_lag_bytes,
		NULL as write_lag_seconds,
		NULL as flush_lag_seconds,
		NULL as replay_lag_seconds
FROM pg_stat_replication`

	//  Query for Postgres versions from 10 and newer.
	postgresReplicationQueryLatest = `SELECT
    pid, coalesce(client_addr, '127.0.0.1') AS client_addr, usename, application_name, state,
    pg_is_in_recovery()::int AS recovery,
		(case pg_is_in_recovery() when 't' then null else pg_current_wal_lsn() - '0/00000000' end) AS wal_bytes,
		pg_current_wal_lsn() - sent_lsn AS pending_lag_bytes,
		sent_lsn - write_lsn AS write_lag_bytes,
		write_lsn - flush_lsn AS flush_lag_bytes,
		flush_lsn - replay_lsn AS replay_lag_bytes,
		pg_current_wal_lsn() - replay_lsn AS total_lag_bytes,
		coalesce(extract(epoch from write_lag), 0) as write_lag_seconds,
		coalesce(extract(epoch from flush_lag), 0) as flush_lag_seconds,
		coalesce(extract(epoch from replay_lag), 0) as replay_lag_seconds
FROM pg_stat_replication`
)

type postgresReplicationCollector struct {
	labelNames []string
	recovery   typedDesc
	wal        typedDesc
	lagbytes   typedDesc
	lagseconds typedDesc
}

// NewPostgresReplicationCollector returns a new Collector exposing postgres replication stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-REPLICATION-VIEW
func NewPostgresReplicationCollector(constLabels prometheus.Labels) (Collector, error) {
	var labelNames = []string{"client_addr", "usename", "application_name", "state", "type"}

	return &postgresReplicationCollector{
		labelNames: labelNames,
		recovery: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "recovery", "state"),
				"Current recovery state, 0 - not in recovery; 1 - in recovery.",
				[]string{}, constLabels,
			), valueType: prometheus.GaugeValue,
		},
		wal: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "wal", "bytes_total"),
				"Total amount of WAL generated, in bytes.",
				[]string{}, constLabels,
			), valueType: prometheus.CounterValue,
		},
		lagbytes: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "replication", "lag_bytes"),
				"Current number of bytes standby is behind than primary.",
				labelNames, constLabels,
			), valueType: prometheus.GaugeValue,
		},
		lagseconds: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "replication", "lag_seconds"),
				"Current number of seconds standby is behind than primary.",
				labelNames, constLabels,
			), valueType: prometheus.GaugeValue,
		},
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresReplicationCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	res, err := conn.Query(selectReplicationQuery(config.ServerVersionNum))
	if err != nil {
		return err
	}

	// parse pg_stat_statements stats
	stats := parsePostgresReplicationStats(res, c.labelNames)

	for _, stat := range stats {
		ch <- c.recovery.mustNewConstMetric(stat.recovery)
		ch <- c.wal.mustNewConstMetric(stat.walBytes)
		ch <- c.lagbytes.mustNewConstMetric(stat.pendingLagBytes, stat.clientaddr, stat.usename, stat.applicationName, stat.state, "pending")
		ch <- c.lagbytes.mustNewConstMetric(stat.writeLagBytes, stat.clientaddr, stat.usename, stat.applicationName, stat.state, "write")
		ch <- c.lagbytes.mustNewConstMetric(stat.flushLagBytes, stat.clientaddr, stat.usename, stat.applicationName, stat.state, "flush")
		ch <- c.lagbytes.mustNewConstMetric(stat.replayLagBytes, stat.clientaddr, stat.usename, stat.applicationName, stat.state, "replay")
		ch <- c.lagbytes.mustNewConstMetric(stat.totalLagBytes, stat.clientaddr, stat.usename, stat.applicationName, stat.state, "total")
		ch <- c.lagseconds.mustNewConstMetric(stat.writeLagSeconds, stat.clientaddr, stat.usename, stat.applicationName, stat.state, "write")
		ch <- c.lagseconds.mustNewConstMetric(stat.flushLagSeconds, stat.clientaddr, stat.usename, stat.applicationName, stat.state, "flush")
		ch <- c.lagseconds.mustNewConstMetric(stat.replayLagSeconds, stat.clientaddr, stat.usename, stat.applicationName, stat.state, "replay")
	}

	return nil
}

// postgresReplicationStat represents per-replica stats based on pg_stat_replication.
type postgresReplicationStat struct {
	pid              string
	clientaddr       string
	usename          string
	applicationName  string
	state            string
	recovery         float64
	walBytes         float64
	pendingLagBytes  float64
	writeLagBytes    float64
	flushLagBytes    float64
	replayLagBytes   float64
	totalLagBytes    float64
	writeLagSeconds  float64
	flushLagSeconds  float64
	replayLagSeconds float64
}

// parsePostgresReplicationStats parses PGResult and returns struct with stats values.
func parsePostgresReplicationStats(r *model.PGResult, labelNames []string) map[string]postgresReplicationStat {
	var stats = make(map[string]postgresReplicationStat)

	for _, row := range r.Rows {
		stat := postgresReplicationStat{}

		// collect label values
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "pid":
				stat.pid = row[i].String
			case "client_addr":
				stat.clientaddr = row[i].String
			case "usename":
				stat.usename = row[i].String
			case "application_name":
				stat.applicationName = row[i].String
			case "state":
				stat.state = row[i].String
			}
		}

		// use pid as key in the map
		pid := stat.pid

		// Put stats with labels (but with no data values yet) into stats store.
		stats[pid] = stat

		// fetch data values from columns
		for i, colname := range r.Colnames {
			// skip columns if its value used as a label
			if stringsContains(labelNames, string(colname.Name)) {
				log.Debug("skip label mapped column")
				continue
			}

			// Skip empty (NULL) values.
			if row[i].String == "" {
				log.Debug("got empty (NULL) value, skip")
				continue
			}

			// Get data value and convert it to float64 used by Prometheus.
			v, err := strconv.ParseFloat(row[i].String, 64)
			if err != nil {
				log.Errorf("skip collecting metric: %s", err)
				continue
			}

			// Run column-specific logic
			switch string(colname.Name) {
			case "recovery":
				s := stats[pid]
				s.recovery = v
				stats[pid] = s
			case "wal_bytes":
				s := stats[pid]
				s.walBytes = v
				stats[pid] = s
			case "pending_lag_bytes":
				s := stats[pid]
				s.pendingLagBytes = v
				stats[pid] = s
			case "write_lag_bytes":
				s := stats[pid]
				s.writeLagBytes = v
				stats[pid] = s
			case "flush_lag_bytes":
				s := stats[pid]
				s.flushLagBytes = v
				stats[pid] = s
			case "replay_lag_bytes":
				s := stats[pid]
				s.replayLagBytes = v
				stats[pid] = s
			case "total_lag_bytes":
				s := stats[pid]
				s.totalLagBytes = v
				stats[pid] = s
			case "write_lag_seconds":
				s := stats[pid]
				s.writeLagSeconds = v
				stats[pid] = s
			case "flush_lag_seconds":
				s := stats[pid]
				s.flushLagSeconds = v
				stats[pid] = s
			case "replay_lag_seconds":
				s := stats[pid]
				s.replayLagSeconds = v
				stats[pid] = s
			default:
				log.Debugf("unsupported pg_stat_replication stat column: %s, skip", string(colname.Name))
				continue
			}
		}
	}

	return stats
}

// selectReplicationQuery returns suitable replication query depending on passed version.
func selectReplicationQuery(version int) string {
	switch {
	case version < PostgresV10:
		return postgresReplicationQuery96
	default:
		return postgresReplicationQueryLatest
	}
}
