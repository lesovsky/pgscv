package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/model"
)

type postgresBgwriterCollector struct {
	builtin []typedDescSet
	custom  []typedDescSet
}

// NewPostgresBgwriterCollector returns a new Collector exposing postgres bgwriter and checkpointer stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-BGWRITER-VIEW
func NewPostgresBgwriterCollector(constLabels prometheus.Labels, settings model.CollectorSettings) (Collector, error) {
	const postgresBgwriterQuery = "SELECT " +
		"checkpoints_timed AS timed, checkpoints_req AS req, " +
		"checkpoint_write_time * 0.001 AS write, checkpoint_sync_time * 0.001 AS sync, " +
		"(checkpoint_write_time + checkpoint_sync_time) * 0.001 AS all_time, " +
		"buffers_checkpoint * (SELECT setting FROM pg_settings WHERE name = 'block_size')::int AS checkpointer, " +
		"buffers_clean * (SELECT setting FROM pg_settings WHERE name = 'block_size')::int AS bgwriter, " +
		"buffers_backend * (SELECT setting FROM pg_settings WHERE name = 'block_size')::int AS backend, " +
		"maxwritten_clean, buffers_backend_fsync, " +
		"buffers_alloc * (SELECT setting FROM pg_settings WHERE name = 'block_size')::int AS allocated, " +
		"coalesce(extract('epoch' from age(now(), stats_reset)), 0) as stats_age_seconds " +
		"FROM pg_stat_bgwriter /* pgSCV */"

	builtinSubsystems := model.Subsystems{
		// Use empty subsystem name.
		// pg_stat_bgwriter view contains stats about multiple subsystems, but Each subsystem
		// uses its own query for requesting stats, but we can request all stats using single
		// query - thus union all metrics in single subsystem with empty name.
		"": {
			Query: postgresBgwriterQuery,
			Metrics: model.Metrics{
				{
					ShortName:     "ckpt_checkpoints_total",
					Usage:         "COUNTER",
					LabeledValues: map[string][]string{"ckpt": {"timed", "req"}},
					Description:   "Total number of checkpoints that have been performed of each type.",
				},
				{
					ShortName:     "ckpt_time_seconds_total",
					Usage:         "COUNTER",
					LabeledValues: map[string][]string{"stage": {"write", "sync"}},
					Description:   "Amount of time that has been spent processing data during checkpoint in each stage, in seconds.",
				},
				{
					ShortName:   "ckpt_time_seconds_all_total",
					Usage:       "COUNTER",
					Value:       "all_time",
					Description: "Total amount of time that has been spent processing data during checkpoint in all stages, in seconds.",
				},
				{
					ShortName:     "written_bytes_total",
					Usage:         "COUNTER",
					LabeledValues: map[string][]string{"process": {"checkpointer", "bgwriter", "backend"}},
					Description:   "Total number of bytes written by each subsystem, in bytes.",
				},
				{
					ShortName:   "bgwriter_maxwritten_clean_total",
					Usage:       "COUNTER",
					Value:       "maxwritten_clean",
					Description: "Total number of times the background writer stopped a cleaning scan because it had written too many buffers.",
				},
				{
					ShortName:   "bgwriter_stats_age_seconds",
					Usage:       "COUNTER",
					Value:       "stats_age_seconds",
					Description: "The age of the background writer activity statistics, in seconds.",
				},
				{
					ShortName:   "backends_fsync_total",
					Usage:       "COUNTER",
					Value:       "buffers_backend_fsync",
					Description: "Total number of times a backends had to execute its own fsync() call.",
				},
				{
					ShortName:   "backends_allocated_bytes_total",
					Usage:       "COUNTER",
					Value:       "allocated",
					Description: "Total number of bytes allocated by backends.",
				},
			},
		},
	}

	removeCollisions(builtinSubsystems, settings.Subsystems)

	return &postgresBgwriterCollector{
		builtin: newDeskSetsFromSubsystems("postgres", builtinSubsystems, constLabels),
		custom:  newDeskSetsFromSubsystems("postgres", settings.Subsystems, constLabels),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresBgwriterCollector) Update(config Config, ch chan<- prometheus.Metric) error {
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
