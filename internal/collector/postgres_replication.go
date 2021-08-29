package collector

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"strconv"
)

const (
	postgresWalQuery96 = "SELECT pg_is_in_recovery()::int AS recovery, " +
		"(case pg_is_in_recovery() when 't' then pg_last_xlog_receive_location() else pg_current_xlog_location() end) - '0/00000000' AS wal_bytes"

	postgresWalQuertLatest = "SELECT pg_is_in_recovery()::int AS recovery, " +
		"(case pg_is_in_recovery() when 't' then pg_last_wal_receive_lsn() else pg_current_wal_lsn() end) - '0/00000000' AS wal_bytes"

	// Query for Postgres version 9.6 and older.
	postgresReplicationQuery96 = "SELECT pid, coalesce(client_addr, '127.0.0.1') AS client_addr, usename AS user, application_name, state, " +
		"pg_current_xlog_location() - sent_location AS pending_lag_bytes, " +
		"sent_location - write_location AS write_lag_bytes, " +
		"write_location - flush_location AS flush_lag_bytes, " +
		"flush_location - replay_location AS replay_lag_bytes, " +
		"pg_current_xlog_location() - replay_location AS total_lag_bytes, " +
		"NULL AS write_lag_seconds, NULL AS flush_lag_seconds, NULL AS replay_lag_seconds, NULL AS total_lag_seconds " +
		"FROM pg_stat_replication"

	// Query for Postgres versions from 10 and newer.
	postgresReplicationQueryLatest = "SELECT pid, coalesce(client_addr, '127.0.0.1') AS client_addr, usename AS user, application_name, state, " +
		"pg_current_wal_lsn() - sent_lsn AS pending_lag_bytes, " +
		"sent_lsn - write_lsn AS write_lag_bytes, " +
		"write_lsn - flush_lsn AS flush_lag_bytes, " +
		"flush_lsn - replay_lsn AS replay_lag_bytes, " +
		"pg_current_wal_lsn() - replay_lsn AS total_lag_bytes, " +
		"coalesce(extract(epoch from write_lag), 0) AS write_lag_seconds, " +
		"coalesce(extract(epoch from flush_lag), 0) AS flush_lag_seconds, " +
		"coalesce(extract(epoch from replay_lag), 0) AS replay_lag_seconds, " +
		"coalesce(extract(epoch from write_lag+flush_lag+replay_lag), 0) AS total_lag_seconds " +
		"FROM pg_stat_replication"
)

type postgresReplicationCollector struct {
	labelNames      []string
	recovery        typedDesc
	wal             typedDesc
	lagbytes        typedDesc
	lagseconds      typedDesc
	lagtotalbytes   typedDesc
	lagtotalseconds typedDesc
}

// NewPostgresReplicationCollector returns a new Collector exposing postgres replication stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-REPLICATION-VIEW
func NewPostgresReplicationCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	var labelNames = []string{"client_addr", "user", "application_name", "state", "lag"}

	return &postgresReplicationCollector{
		labelNames: labelNames,
		recovery: newBuiltinTypedDesc(
			descOpts{"postgres", "recovery", "info", "Current recovery state, 0 - not in recovery; 1 - in recovery.", 0},
			prometheus.GaugeValue,
			nil, constLabels,
			settings.Filters,
		),
		wal: newBuiltinTypedDesc(
			descOpts{"postgres", "wal", "written_bytes_total", "Total amount of WAL written (or received in case of standby), in bytes.", 0},
			prometheus.CounterValue,
			nil, constLabels,
			settings.Filters,
		),
		lagbytes: newBuiltinTypedDesc(
			descOpts{"postgres", "replication", "lag_bytes", "Number of bytes standby is behind than primary in each WAL processing phase.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		lagseconds: newBuiltinTypedDesc(
			descOpts{"postgres", "replication", "lag_seconds", "Number of seconds standby is behind than primary in each WAL processing phase.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		lagtotalbytes: newBuiltinTypedDesc(
			descOpts{"postgres", "replication", "lag_all_bytes", "Number of bytes standby is behind than primary including all phases.", 0},
			prometheus.GaugeValue,
			[]string{"client_addr", "user", "application_name", "state"}, constLabels,
			settings.Filters,
		),
		lagtotalseconds: newBuiltinTypedDesc(
			descOpts{"postgres", "replication", "lag_all_seconds", "Number of seconds standby is behind than primary including all phases.", 0},
			prometheus.GaugeValue,
			[]string{"client_addr", "user", "application_name", "state"}, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresReplicationCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Get recovery state.
	var recovery int
	var walBytes int64
	err = conn.Conn().QueryRow(context.TODO(), selectWalQuery(config.serverVersionNum)).Scan(&recovery, &walBytes)
	if err != nil {
		log.Warnf("get recovery state failed: %s; skip", err)
	} else {
		ch <- c.recovery.newConstMetric(float64(recovery))
		ch <- c.wal.newConstMetric(float64(walBytes))
	}

	// Get replication stats.
	res, err := conn.Query(selectReplicationQuery(config.serverVersionNum))
	if err != nil {
		return err
	}

	// Parse pg_stat_replication stats.
	stats := parsePostgresReplicationStats(res, c.labelNames)

	for _, stat := range stats {
		if value, ok := stat.values["pending_lag_bytes"]; ok {
			ch <- c.lagbytes.newConstMetric(value, stat.clientaddr, stat.user, stat.applicationName, stat.state, "pending")
		}
		if value, ok := stat.values["write_lag_bytes"]; ok {
			ch <- c.lagbytes.newConstMetric(value, stat.clientaddr, stat.user, stat.applicationName, stat.state, "write")
		}
		if value, ok := stat.values["flush_lag_bytes"]; ok {
			ch <- c.lagbytes.newConstMetric(value, stat.clientaddr, stat.user, stat.applicationName, stat.state, "flush")
		}
		if value, ok := stat.values["replay_lag_bytes"]; ok {
			ch <- c.lagbytes.newConstMetric(value, stat.clientaddr, stat.user, stat.applicationName, stat.state, "replay")
		}
		if value, ok := stat.values["write_lag_seconds"]; ok {
			ch <- c.lagseconds.newConstMetric(value, stat.clientaddr, stat.user, stat.applicationName, stat.state, "write")
		}
		if value, ok := stat.values["flush_lag_seconds"]; ok {
			ch <- c.lagseconds.newConstMetric(value, stat.clientaddr, stat.user, stat.applicationName, stat.state, "flush")
		}
		if value, ok := stat.values["replay_lag_seconds"]; ok {
			ch <- c.lagseconds.newConstMetric(value, stat.clientaddr, stat.user, stat.applicationName, stat.state, "replay")
		}
		if value, ok := stat.values["total_lag_bytes"]; ok {
			ch <- c.lagtotalbytes.newConstMetric(value, stat.clientaddr, stat.user, stat.applicationName, stat.state)
		}
		if value, ok := stat.values["total_lag_seconds"]; ok {
			ch <- c.lagtotalseconds.newConstMetric(value, stat.clientaddr, stat.user, stat.applicationName, stat.state)
		}
	}

	return nil
}

// postgresReplicationStat represents per-replica stats based on pg_stat_replication.
type postgresReplicationStat struct {
	pid             string
	clientaddr      string
	user            string
	applicationName string
	state           string
	values          map[string]float64
}

// parsePostgresReplicationStats parses PGResult and returns struct with stats values.
func parsePostgresReplicationStats(r *model.PGResult, labelNames []string) map[string]postgresReplicationStat {
	log.Debug("parse postgres replication stats")

	var stats = make(map[string]postgresReplicationStat)

	for _, row := range r.Rows {
		stat := postgresReplicationStat{values: map[string]float64{}}

		// collect label values
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "pid":
				stat.pid = row[i].String
			case "client_addr":
				stat.clientaddr = row[i].String
			case "user":
				stat.user = row[i].String
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
				continue
			}

			// Skip empty (NULL) values.
			if !row[i].Valid {
				continue
			}

			// Get data value and convert it to float64 used by Prometheus.
			v, err := strconv.ParseFloat(row[i].String, 64)
			if err != nil {
				log.Errorf("invalid input, parse '%s' failed: %s; skip", row[i].String, err)
				continue
			}

			s := stats[pid]

			// Run column-specific logic
			switch string(colname.Name) {
			case "pending_lag_bytes":
				s.values["pending_lag_bytes"] = v
			case "write_lag_bytes":
				s.values["write_lag_bytes"] = v
			case "flush_lag_bytes":
				s.values["flush_lag_bytes"] = v
			case "replay_lag_bytes":
				s.values["replay_lag_bytes"] = v
			case "write_lag_seconds":
				s.values["write_lag_seconds"] = v
			case "flush_lag_seconds":
				s.values["flush_lag_seconds"] = v
			case "replay_lag_seconds":
				s.values["replay_lag_seconds"] = v
			case "total_lag_bytes":
				s.values["total_lag_bytes"] = v
			case "total_lag_seconds":
				s.values["total_lag_seconds"] = v
			default:
				continue
			}

			stats[pid] = s
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

// selectWalQuery returns suitable wal state query depending on passed version.
func selectWalQuery(version int) string {
	switch {
	case version < PostgresV10:
		return postgresWalQuery96
	default:
		return postgresWalQuertLatest
	}
}
