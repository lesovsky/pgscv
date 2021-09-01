package collector

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
)

const (
	postgresWalQuery96 = "SELECT pg_is_in_recovery()::int AS recovery, " +
		"(case pg_is_in_recovery() when 't' then pg_last_xlog_receive_location() else pg_current_xlog_location() end) - '0/00000000' AS wal_bytes"

	postgresWalQuertLatest = "SELECT pg_is_in_recovery()::int AS recovery, " +
		"(case pg_is_in_recovery() when 't' then pg_last_wal_receive_lsn() else pg_current_wal_lsn() end) - '0/00000000' AS wal_bytes"
)

type postgresWalCollector struct {
	labelNames []string
	recovery   typedDesc
	wal        typedDesc
}

// NewPostgresWalCollector returns a new Collector exposing postgres WAL stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-WAL-VIEW
func NewPostgresWalCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	var labelNames = []string{"client_addr", "user", "application_name", "state", "lag"}

	return &postgresWalCollector{
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
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresWalCollector) Update(config Config, ch chan<- prometheus.Metric) error {
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

	return nil
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
