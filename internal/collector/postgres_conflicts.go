package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"strconv"
)

const (
	postgresDatabaseConflictsQuery = "SELECT datname," +
		"nullif(confl_tablespace, 0) AS confl_tablespace," +
		"nullif(confl_lock, 0) AS confl_lock," +
		"nullif(confl_snapshot, 0) AS confl_snapshot," +
		"nullif(confl_bufferpin, 0) AS confl_bufferpin," +
		"nullif(confl_deadlock, 0) AS confl_deadlock " +
		"FROM pg_stat_database_conflicts"
)

type postgresConflictsCollector struct {
	labelNames []string
	conflicts  typedDesc
}

// NewPostgresConflictsCollector returns a new Collector exposing postgres databases recovery conflicts stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-DATABASE-CONFLICTS-VIEW
func NewPostgresConflictsCollector(constLabels prometheus.Labels, _ model.CollectorSettings) (Collector, error) {
	var labelNames = []string{"datname", "reason"}

	return &postgresConflictsCollector{
		labelNames: labelNames,
		conflicts: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "recovery", "conflicts_total"),
				"Total number of recovery conflicts occurred by each conflict type.",
				labelNames, constLabels,
			), valueType: prometheus.CounterValue,
		},
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

	stats := parsePostgresConflictStats(res, c.labelNames)

	for _, stat := range stats {
		// avoid zero-value metric spam
		if stat.tablespace > 0 {
			ch <- c.conflicts.mustNewConstMetric(stat.tablespace, stat.datname, "tablespace")
		}
		if stat.lock > 0 {
			ch <- c.conflicts.mustNewConstMetric(stat.lock, stat.datname, "lock")
		}
		if stat.snapshot > 0 {
			ch <- c.conflicts.mustNewConstMetric(stat.snapshot, stat.datname, "snapshot")
		}
		if stat.bufferpin > 0 {
			ch <- c.conflicts.mustNewConstMetric(stat.bufferpin, stat.datname, "bufferpin")
		}
		if stat.deadlock > 0 {
			ch <- c.conflicts.mustNewConstMetric(stat.deadlock, stat.datname, "deadlock")
		}
	}

	return nil
}

// postgresConflictStat represents per-database recovery conflicts stats based on pg_stat_database_conflicts.
type postgresConflictStat struct {
	datname    string
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
			case "datname":
				stat.datname = row[i].String
			}
		}

		// Define a map key as a database name.
		databaseFQName := stat.datname

		// Put stats with labels (but with no data values yet) into stats store.
		stats[databaseFQName] = stat

		// fetch data values from columns
		for i, colname := range r.Colnames {
			// skip columns if its value used as a label
			if stringsContains(labelNames, string(colname.Name)) {
				log.Debugf("skip label mapped column '%s'", string(colname.Name))
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

			// Run column-specific logic
			switch string(colname.Name) {
			case "confl_tablespace":
				s := stats[databaseFQName]
				s.tablespace = v
				stats[databaseFQName] = s
			case "confl_lock":
				s := stats[databaseFQName]
				s.lock = v
				stats[databaseFQName] = s
			case "confl_snapshot":
				s := stats[databaseFQName]
				s.snapshot = v
				stats[databaseFQName] = s
			case "confl_bufferpin":
				s := stats[databaseFQName]
				s.bufferpin = v
				stats[databaseFQName] = s
			case "confl_deadlock":
				s := stats[databaseFQName]
				s.deadlock = v
				stats[databaseFQName] = s
			default:
				log.Debugf("unsupported pg_stat_database_conflicts stat column: %s, skip", string(colname.Name))
				continue
			}
		}
	}

	return stats
}
