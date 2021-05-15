package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/model"
)

// postgresLocksCollector is a collector with locks related metrics descriptors.
type postgresLocksCollector struct {
	builtin []typedDescSet
	custom  []typedDescSet
}

// NewPostgresLocksCollector creates new postgresLocksCollector.
func NewPostgresLocksCollector(constLabels prometheus.Labels, settings model.CollectorSettings) (Collector, error) {
	const locksQuery = "SELECT " +
		"count(*) FILTER (WHERE mode = 'AccessShareLock') AS access_share_lock, " +
		"count(*) FILTER (WHERE mode = 'RowShareLock') AS row_share_lock, " +
		"count(*) FILTER (WHERE mode = 'RowExclusiveLock') AS row_exclusive_lock, " +
		"count(*) FILTER (WHERE mode = 'ShareUpdateExclusiveLock') AS share_update_exclusive_lock, " +
		"count(*) FILTER (WHERE mode = 'ShareLock') AS share_lock, " +
		"count(*) FILTER (WHERE mode = 'ShareRowExclusiveLock') AS share_row_exclusive_lock, " +
		"count(*) FILTER (WHERE mode = 'ExclusiveLock') AS exclusive_lock, " +
		"count(*) FILTER (WHERE mode = 'AccessExclusiveLock') AS access_exclusive_lock, " +
		"count(*) FILTER (WHERE not granted) AS not_granted, " +
		"count(*) AS total " +
		"FROM pg_locks"

	builtinSubsystems := model.Subsystems{
		"locks": {
			Query: locksQuery,
			Metrics: model.Metrics{
				{
					ShortName:   "all_in_flight",
					Usage:       "GAUGE",
					Value:       "total",
					Description: "Total number of in-flight locks held by all active processes.",
				},
				{
					ShortName:   "not_granted_in_flight",
					Usage:       "GAUGE",
					Value:       "not_granted",
					Description: "Total number of not granted in-flight locks held by all active processes.",
				},
				{
					ShortName: "in_flight",
					Usage:     "GAUGE",
					LabeledValues: map[string][]string{
						"mode": {
							"access_share_lock/AccessShareLock",
							"row_share_lock/RowShareLock",
							"row_exclusive_lock/RowExclusiveLock",
							"share_update_exclusive_lock/ShareUpdateExclusiveLock",
							"share_lock/ShareLock",
							"share_row_exclusive_lock/ShareRowExclusiveLock",
							"exclusive_lock/ExclusiveLock",
							"access_exclusive_lock/AccessExclusiveLock",
						},
					},
					Description: "Number of in-flight locks held by active processes in each mode.",
				},
			},
		},
	}

	removeCollisions(builtinSubsystems, settings.Subsystems)

	return &postgresLocksCollector{
		builtin: newDeskSetsFromSubsystems("postgres", builtinSubsystems, constLabels),
		custom:  newDeskSetsFromSubsystems("postgres", settings.Subsystems, constLabels),
	}, nil
}

// Update method collects locks metrics.
func (c *postgresLocksCollector) Update(config Config, ch chan<- prometheus.Metric) error {
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
