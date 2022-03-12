package collector

import (
	"github.com/lesovsky/pgscv/internal/log"
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/lesovsky/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
)

const (
	postgresDatabaseConflictsQuery = "SELECT datname AS database, confl_tablespace, confl_lock, confl_snapshot, confl_bufferpin, confl_deadlock FROM pg_stat_database_conflicts where pg_is_in_recovery() = 't'"
)

type postgresConflictsCollector struct {
	conflicts typedDesc
}

// NewPostgresConflictsCollector returns a new Collector exposing postgres databases recovery conflicts stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-DATABASE-CONFLICTS-VIEW
func NewPostgresConflictsCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &postgresConflictsCollector{
		conflicts: newBuiltinTypedDesc(
			descOpts{"postgres", "recovery", "conflicts_total", "Total number of recovery conflicts occurred by each conflict type.", 0},
			prometheus.CounterValue,
			[]string{"database", "conflict"}, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresConflictsCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	res, err := conn.Query(postgresDatabaseConflictsQuery)
	if err != nil {
		return err
	}

	stats := parsePostgresConflictStats(res, c.conflicts.labelNames)

	for _, stat := range stats {
		ch <- c.conflicts.newConstMetric(stat.tablespace, stat.database, "tablespace")
		ch <- c.conflicts.newConstMetric(stat.lock, stat.database, "lock")
		ch <- c.conflicts.newConstMetric(stat.snapshot, stat.database, "snapshot")
		ch <- c.conflicts.newConstMetric(stat.bufferpin, stat.database, "bufferpin")
		ch <- c.conflicts.newConstMetric(stat.deadlock, stat.database, "deadlock")
	}

	return nil
}

// postgresConflictStat represents per-database recovery conflicts stats based on pg_stat_database_conflicts.
type postgresConflictStat struct {
	database   string
	tablespace float64
	lock       float64
	snapshot   float64
	bufferpin  float64
	deadlock   float64
}

// parsePostgresDatabasesStats parses PGResult, extract data and return struct with stats values.
func parsePostgresConflictStats(r *model.PGResult, labelNames []string) map[string]postgresConflictStat {
	log.Debug("parse postgres database conflicts stats")

	var stats = make(map[string]postgresConflictStat)

	// process row by row
	for _, row := range r.Rows {
		stat := postgresConflictStat{}

		// collect label values
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "database":
				stat.database = row[i].String
			}
		}

		// Define a map key as a database name.
		databaseFQName := stat.database

		// Put stats with labels (but with no data values yet) into stats store.
		stats[databaseFQName] = stat

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

			s := stats[databaseFQName]

			// Run column-specific logic
			switch string(colname.Name) {
			case "confl_tablespace":
				s.tablespace = v
			case "confl_lock":
				s.lock = v
			case "confl_snapshot":
				s.snapshot = v
			case "confl_bufferpin":
				s.bufferpin = v
			case "confl_deadlock":
				s.deadlock = v
			default:
				continue
			}

			stats[databaseFQName] = s
		}
	}

	return stats
}
