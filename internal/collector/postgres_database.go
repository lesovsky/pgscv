package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/model"
)

const (
	databaseQuery = "SELECT " +
		"COALESCE(datname, '__shared__') AS datname, " +
		"xact_commit, xact_rollback, " +
		"blks_read, blks_hit , tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted, " +
		"conflicts, temp_files, temp_bytes, deadlocks, " +
		"blk_read_time * 0.001 AS read_t, blk_write_time * 0.001 AS write_t, " +
		"pg_database_size(datname) AS size_bytes, " +
		"coalesce(extract('epoch' from age(now(), stats_reset)), 0) AS stats_age_seconds " +
		"FROM pg_stat_database WHERE datname IN (SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate)"

	xidLimitQuery = "SELECT 'database' AS xid, 2147483647 - greatest(max(age(datfrozenxid)), max(age(coalesce(nullif(datminmxid, 1), datfrozenxid)))) AS to_limit FROM pg_database " +
		"UNION SELECT 'prepared_xacts' AS xid, 2147483647 - coalesce(max(age(transaction)), 0) AS to_limit FROM pg_prepared_xacts " +
		"UNION SELECT 'replication_slots' AS xid, 2147483647 - greatest(coalesce(min(age(xmin)), 0), coalesce(min(age(catalog_xmin)), 0)) AS to_limit FROM pg_replication_slots"
)

type postgresDatabasesCollector struct {
	builtin []typedDescSet
	custom  []typedDescSet
}

// NewPostgresDatabasesCollector returns a new Collector exposing postgres databases stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-DATABASE-VIEW
func NewPostgresDatabasesCollector(constLabels prometheus.Labels, settings model.CollectorSettings) (Collector, error) {
	builtinSubsystems := model.Subsystems{
		"database": {
			Query: databaseQuery,
			Metrics: model.Metrics{
				{
					ShortName:   "xact_commits_total",
					Usage:       "COUNTER",
					Value:       "xact_commit",
					Labels:      []string{"datname"},
					Description: "Total number of transactions had been committed.",
				},
				{
					ShortName:   "xact_rollbacks_total",
					Usage:       "COUNTER",
					Value:       "xact_rollback",
					Labels:      []string{"datname"},
					Description: "Total number of transactions had been rolled back.",
				},
				{
					ShortName:   "conflicts_total",
					Usage:       "COUNTER",
					Value:       "conflicts",
					Labels:      []string{"datname"},
					Description: "Total number of recovery conflicts occurred.",
				},
				{
					ShortName:   "deadlocks_total",
					Usage:       "COUNTER",
					Value:       "deadlocks",
					Labels:      []string{"datname"},
					Description: "Total number of deadlocks occurred.",
				},
				{
					ShortName: "blocks_total",
					Usage:     "COUNTER",
					LabeledValues: map[string][]string{
						"access": {
							"blks_read/read",
							"blks_hit/hit",
						},
					},
					Labels:      []string{"datname"},
					Description: "Total number of disk blocks had been accessed by each type of access.",
				},
				{
					ShortName: "tuples_total",
					Usage:     "COUNTER",
					LabeledValues: map[string][]string{
						"op": {
							"tup_returned/returned",
							"tup_fetched/fetched",
							"tup_inserted/inserted",
							"tup_updated/updated",
							"tup_deleted/deleted",
						},
					},
					Labels:      []string{"datname"},
					Description: "Total number of rows processed by each type of operation.",
				},
				{
					ShortName:   "temp_files_total",
					Usage:       "COUNTER",
					Value:       "temp_files",
					Labels:      []string{"datname"},
					Description: "Total number of temporary files created by queries.",
				},
				{
					ShortName:   "temp_bytes_total",
					Usage:       "COUNTER",
					Value:       "temp_bytes",
					Labels:      []string{"datname"},
					Description: "Total amount of data written to temporary files by queries.",
				},
				{
					ShortName: "blk_time_seconds",
					Usage:     "COUNTER",
					LabeledValues: map[string][]string{
						"type": {
							"read_t/read",
							"write_t/write",
						},
					},
					Labels:      []string{"datname"},
					Description: "Time spent accessing data file blocks by backends in this database in each access type, in seconds.",
				},
				{
					ShortName:   "size_bytes",
					Usage:       "GAUGE",
					Value:       "size_bytes",
					Labels:      []string{"datname"},
					Description: "Total size of the database, in bytes.",
				},
				{
					ShortName:   "stats_age_seconds",
					Usage:       "COUNTER",
					Value:       "stats_age_seconds",
					Labels:      []string{"datname"},
					Description: "The age of the activity statistics, in seconds.",
				},
			},
		},
		"xacts": {
			Query: xidLimitQuery,
			Metrics: model.Metrics{
				{
					ShortName:   "left_before_wraparound",
					Usage:       "GAUGE",
					Value:       "to_limit",
					Labels:      []string{"xid"},
					Description: "The least number of transactions (among all databases) left before force shutdown due to XID wraparound.",
				},
			},
		},
	}

	removeCollisions(builtinSubsystems, settings.Subsystems)

	return &postgresDatabasesCollector{
		builtin: newDeskSetsFromSubsystems("postgres", builtinSubsystems, constLabels),
		custom:  newDeskSetsFromSubsystems("postgres", settings.Subsystems, constLabels),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresDatabasesCollector) Update(config Config, ch chan<- prometheus.Metric) error {
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
