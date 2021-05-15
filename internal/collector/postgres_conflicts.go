package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/model"
)

const (
	postgresDatabaseConflictsQuery = "SELECT datname," +
		"confl_tablespace AS tablespace," +
		"confl_lock AS lock," +
		"confl_snapshot AS snapshot," +
		"confl_bufferpin AS bufferpin," +
		"confl_deadlock AS deadlock " +
		"FROM pg_stat_database_conflicts"
)

type postgresConflictsCollector struct {
	//labelNames []string
	//conflicts  typedDesc
	builtin []typedDescSet
	custom  []typedDescSet
}

// NewPostgresConflictsCollector returns a new Collector exposing postgres databases recovery conflicts stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-DATABASE-CONFLICTS-VIEW
func NewPostgresConflictsCollector(constLabels prometheus.Labels, settings model.CollectorSettings) (Collector, error) {
	builtinSubsystems := model.Subsystems{
		"recovery": {
			Query: postgresDatabaseConflictsQuery,
			Metrics: model.Metrics{
				{
					ShortName:     "conflicts_total",
					Usage:         "COUNTER",
					LabeledValues: map[string][]string{"conflict": {"tablespace", "lock", "snapshot", "bufferpin", "deadlock"}},
					Labels:        []string{"datname"},
					Description:   "Total number of recovery conflicts occurred by each conflict type.",
				},
			},
		},
	}

	removeCollisions(builtinSubsystems, settings.Subsystems)

	return &postgresConflictsCollector{
		builtin: newDeskSetsFromSubsystems("postgres", builtinSubsystems, constLabels),
		custom:  newDeskSetsFromSubsystems("postgres", settings.Subsystems, constLabels),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresConflictsCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	// TODO: update only when Postgres is in recovery.

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
