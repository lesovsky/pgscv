package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
)

// TODO: убрать подзапрос c определением wal_segment_size и заменить на использование значения из конфига

const walArchivingNewQuery = "SELECT archived_count AS archived_total, " +
	"failed_count AS failed_total, " +
	"extract(epoch from now() - last_archived_time) AS since_last_archive_seconds, " +
	"(SELECT count(*) * (SELECT setting FROM pg_settings WHERE name = 'wal_segment_size')::int FROM pg_ls_archive_statusdir() WHERE name ~'.ready') AS lag_bytes " +
	"FROM pg_stat_archiver WHERE archived_count > 0"

// postgresWalArchivingCollector implements Collector interface.
type postgresWalArchivingCollector struct {
	builtin []typedDescSet
	custom  []typedDescSet
}

// NewPostgresWalArchivingCollector returns a new Collector exposing postgres WAL archiving stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ARCHIVER-VIEW
func NewPostgresWalArchivingCollector(constLabels prometheus.Labels, settings model.CollectorSettings) (Collector, error) {
	builtinSubsystems := model.Subsystems{
		"archiver": {
			Query: walArchivingNewQuery,
			Metrics: model.Metrics{
				{
					ShortName:   "archived_total",
					Usage:       "COUNTER",
					Description: "Total number of WAL segments had been successfully archived.",
				},
				{
					ShortName:   "failed_total",
					Usage:       "COUNTER",
					Description: "Total number of attempts when WAL segments had been failed to archive.",
				},
				{
					ShortName:   "since_last_archive_seconds",
					Usage:       "GAUGE",
					Description: "Number of seconds since last WAL segment had been successfully archived.",
				},
				{
					ShortName:   "lag_bytes",
					Usage:       "GAUGE",
					Description: "Amount of WAL segments ready, but not archived, in bytes.",
				},
			},
		},
	}

	removeCollisions(builtinSubsystems, settings.Subsystems)

	return &postgresWalArchivingCollector{
		builtin: newDeskSetsFromSubsystems("postgres", builtinSubsystems, constLabels),
		custom:  newDeskSetsFromSubsystems("postgres", settings.Subsystems, constLabels),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresWalArchivingCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	if config.ServerVersionNum < PostgresV12 {
		log.Debugln("[postgres WAL archiver collector]: some system functions are not available, required Postgres 12 or newer")
		return nil
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
