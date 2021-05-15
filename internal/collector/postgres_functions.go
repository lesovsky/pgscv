package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/model"
)

const postgresFunctionsQuery = "SELECT schemaname, funcname, calls, " +
	"total_time * 0.001 AS total_time, self_time * 0.001 AS self_time " +
	"FROM pg_stat_user_functions"

type postgresFunctionsCollector struct {
	builtin []typedDescSet
	custom  []typedDescSet
}

// NewPostgresFunctionsCollector returns a new Collector exposing postgres SQL functions stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-USER-FUNCTIONS-VIEW
func NewPostgresFunctionsCollector(constLabels prometheus.Labels, settings model.CollectorSettings) (Collector, error) {
	builtinSubsystems := model.Subsystems{
		"function": {
			Databases: ".+", // collect metrics from all databases
			Query:     postgresFunctionsQuery,
			Metrics: model.Metrics{
				{
					ShortName:   "calls_total",
					Usage:       "COUNTER",
					Value:       "calls",
					Labels:      []string{"schemaname", "funcname"},
					Description: "Total number of times functions had been called.",
				},
				{
					ShortName:   "total_time_seconds",
					Usage:       "COUNTER",
					Value:       "total_time",
					Labels:      []string{"schemaname", "funcname"},
					Description: "Total time spent in function and all other functions called by it, in seconds.",
				},
				{
					ShortName:   "self_time_seconds",
					Usage:       "COUNTER",
					Value:       "self_time",
					Labels:      []string{"schemaname", "funcname"},
					Description: "Total time spent in function itself, not including other functions called by it, in seconds.",
				},
			},
		},
	}

	removeCollisions(builtinSubsystems, settings.Subsystems)

	return &postgresFunctionsCollector{
		builtin: newDeskSetsFromSubsystems("postgres", builtinSubsystems, constLabels),
		custom:  newDeskSetsFromSubsystems("postgres", settings.Subsystems, constLabels),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresFunctionsCollector) Update(config Config, ch chan<- prometheus.Metric) error {
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
