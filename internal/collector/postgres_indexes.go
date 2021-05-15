package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/model"
)

// postgresIndexesCollector defines metric descriptors and stats store.
type postgresIndexesCollector struct {
	builtin []typedDescSet
	custom  []typedDescSet
}

// NewPostgresIndexesCollector returns a new Collector exposing postgres indexes stats.
// For details see
// https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ALL-INDEXES-VIEW
// https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STATIO-ALL-INDEXES-VIEW
func NewPostgresIndexesCollector(constLabels prometheus.Labels, settings model.CollectorSettings) (Collector, error) {
	const userIndexesQuery = "SELECT schemaname, relname, indexrelname, (i.indisprimary OR i.indisunique) AS key," +
		"idx_scan, idx_tup_read, idx_tup_fetch, idx_blks_read, idx_blks_hit, pg_relation_size(s1.indexrelid) AS size_bytes " +
		"FROM pg_stat_user_indexes s1 " +
		"JOIN pg_statio_user_indexes s2 USING (schemaname, relname, indexrelname) " +
		"JOIN pg_index i ON (s1.indexrelid = i.indexrelid) " +
		"WHERE NOT EXISTS (SELECT 1 FROM pg_locks WHERE relation = s1.indexrelid AND mode = 'AccessExclusiveLock' AND granted)"

	// Use empty subsystem name.
	// pg_stat_bgwriter view contains stats about multiple subsystems, but Each subsystem
	// uses its own query for requesting stats, but we can request all stats using single
	// query - thus union all metrics in single subsystem with empty name.
	builtinSubsystems := model.Subsystems{
		"": {
			Databases: ".+", // collect metrics from all databases
			Query:     userIndexesQuery,
			Metrics: model.Metrics{
				{
					ShortName:   "index_scans_total",
					Usage:       "COUNTER",
					Value:       "idx_scan",
					Labels:      []string{"schemaname", "relname", "indexrelname", "key"},
					Description: "",
				},
				{
					ShortName: "index_tuples_total",
					Usage:     "COUNTER",
					LabeledValues: map[string][]string{
						"op": {
							"idx_tup_read/read",
							"idx_tup_fetch/fetched",
						},
					},
					Labels:      []string{"schemaname", "relname", "indexrelname"},
					Description: "Total number of index entries processed by scans.",
				},
				{
					ShortName:   "index_size_bytes",
					Usage:       "GAUGE",
					Value:       "size_bytes",
					Labels:      []string{"schemaname", "relname", "indexrelname"},
					Description: "Total size of the index, in bytes.",
				},
				{
					ShortName: "index_io_blocks_total",
					Usage:     "COUNTER",
					LabeledValues: map[string][]string{
						"cache_hit": {
							"idx_blks_read/false",
							"idx_blks_hit/true",
						},
					},
					Labels:      []string{"schemaname", "relname", "indexrelname"},
					Description: "Total number of indexes' blocks processed.",
				},
			},
		},
	}

	removeCollisions(builtinSubsystems, settings.Subsystems)

	return &postgresIndexesCollector{
		builtin: newDeskSetsFromSubsystems("postgres", builtinSubsystems, constLabels),
		custom:  newDeskSetsFromSubsystems("postgres", settings.Subsystems, constLabels),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresIndexesCollector) Update(config Config, ch chan<- prometheus.Metric) error {
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
