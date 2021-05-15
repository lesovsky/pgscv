package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
)

const (
	postgresWalQuery96 = "SELECT pg_is_in_recovery()::int AS recovery, " +
		"(case pg_is_in_recovery() when 't' then pg_last_xlog_receive_location() else pg_current_xlog_location() end) - '0/00000000' AS wal_bytes"

	postgresWalQuertLatest = "SELECT pg_is_in_recovery()::int AS recovery, " +
		"(case pg_is_in_recovery() when 't' then pg_last_wal_receive_lsn() else pg_current_wal_lsn() end) - '0/00000000' AS wal_bytes"

	// Query for Postgres version 9.6 and older.
	postgresReplicationQuery96 = "SELECT pid, coalesce(client_addr, '127.0.0.1') AS client_addr, usename, application_name, state, " +
		"pg_current_xlog_location() - sent_location AS pending_lag_bytes, " +
		"sent_location - write_location AS write_lag_bytes, " +
		"write_location - flush_location AS flush_lag_bytes, " +
		"flush_location - replay_location AS replay_lag_bytes, " +
		"pg_current_xlog_location() - replay_location AS total_lag_bytes, " +
		"NULL AS write_lag_seconds, NULL AS flush_lag_seconds, NULL AS replay_lag_seconds, NULL AS total_lag_seconds " +
		"FROM pg_stat_replication"

	// Query for Postgres versions from 10 and newer.
	postgresReplicationQueryLatest = "SELECT pid, coalesce(client_addr, '127.0.0.1') AS client_addr, usename, application_name, state, " +
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
	builtin []typedDescSet
	custom  []typedDescSet
}

// NewPostgresReplicationCollector returns a new Collector exposing postgres replication stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-REPLICATION-VIEW
func NewPostgresReplicationCollector(constLabels prometheus.Labels, settings model.CollectorSettings) (Collector, error) {
	builtinSubsystems := model.Subsystems{
		"replication": {
			Query: "",
			Metrics: model.Metrics{
				{
					ShortName: "lag_bytes",
					Usage:     "GAUGE",
					LabeledValues: map[string][]string{
						"lag": {
							"pending_lag_bytes/pending",
							"write_lag_bytes/write",
							"flush_lag_bytes/flush",
							"replay_lag_bytes/replay",
						},
					},
					Labels:      []string{"client_addr", "usename", "application_name", "state"},
					Description: "Number of bytes standby is behind than primary in each WAL processing phase.",
				},
				{
					ShortName:   "lag_total_bytes",
					Usage:       "GAUGE",
					Value:       "total_lag_bytes",
					Labels:      []string{"client_addr", "usename", "application_name", "state"},
					Description: "Number of bytes standby is behind than primary including all phases.",
				},
				{
					ShortName: "lag_seconds",
					Usage:     "GAUGE",
					LabeledValues: map[string][]string{
						"lag": {
							"write_lag_seconds/write",
							"flush_lag_seconds/flush",
							"replay_lag_seconds/replay",
						},
					},
					Labels:      []string{"client_addr", "usename", "application_name", "state"},
					Description: "Number of seconds standby is behind than primary in each WAL processing phase.",
				},
				{
					ShortName:   "lag_total_seconds",
					Usage:       "GAUGE",
					Value:       "total_lag_seconds",
					Labels:      []string{"client_addr", "usename", "application_name", "state"},
					Description: "Number of seconds standby is behind than primary including all phases.",
				},
			},
		},
		"wal": {
			Query: "",
			Metrics: model.Metrics{
				{
					ShortName:   "bytes_total",
					Usage:       "COUNTER",
					Value:       "wal_bytes",
					Description: "Total amount of WAL generated or received, in bytes.",
				},
			},
		},
		"recovery": {
			Query: "",
			Metrics: model.Metrics{
				{
					ShortName:   "info",
					Usage:       "GAUGE",
					Value:       "recovery",
					Description: "Current recovery state, 0 - not in recovery; 1 - in recovery.",
				},
			},
		},
	}

	removeCollisions(builtinSubsystems, settings.Subsystems)

	return &postgresReplicationCollector{
		builtin: newDeskSetsFromSubsystems("postgres", builtinSubsystems, constLabels),
		custom:  newDeskSetsFromSubsystems("postgres", settings.Subsystems, constLabels),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresReplicationCollector) Update(config Config, ch chan<- prometheus.Metric) error {

	// Adjust queries depending on PostgreSQL version.
	for i, subsys := range c.builtin {
		switch subsys.subsystem {
		case "replication":
			c.builtin[i].query = selectReplicationQuery(config.ServerVersionNum)
		case "wal", "recovery":
			c.builtin[i].query = selectWalQuery(config.ServerVersionNum)
		default:
			log.Warnf("unknown builtin subsystem '%s/%s' found; skip", subsys.namespace, subsys.subsystem)
		}
	}

	// Update builtin metrics.
	err := updateAllDescSets(config, c.builtin, ch)
	if err != nil {
		return err
	}

	// Update user-defined metrics.
	err = updateAllDescSets(config, c.custom, ch)
	if err != nil {
		return err
	}

	return nil
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
