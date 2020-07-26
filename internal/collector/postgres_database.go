package collector

import (
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

const databaseQuery = `SELECT
  COALESCE(datname, '__shared__') AS datname,
  xact_commit, xact_rollback,
  blks_read, blks_hit,
  tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted,
  conflicts, temp_files, temp_bytes, deadlocks,
  blk_read_time, blk_write_time,
  pg_database_size(datname) as size_bytes,
  coalesce(extract('epoch' from age(now(), stats_reset)), 0) as stats_age_seconds
FROM pg_stat_database WHERE datname IN (SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate)`

type postgresDatabasesCollector struct {
	descs      []typedDesc
	labelNames []string
}

// NewPostgresDatabasesCollector returns a new Collector exposing postgres databases stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-DATABASE-VIEW
func NewPostgresDatabasesCollector(constLabels prometheus.Labels) (Collector, error) {
	var databaseLabelNames = []string{"datname"}

	return &postgresDatabasesCollector{
		labelNames: databaseLabelNames,
		descs: []typedDesc{
			{
				colname: "xact_commit",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "database", "xact_commit_total"),
					"The total number of transactions committed.",
					databaseLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "xact_rollback",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "database", "xact_rollback_total"),
					"The total number of transactions rolled back.",
					databaseLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "blks_read",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "database", "blks_read_total"),
					"Total number of disk blocks read in this database.",
					databaseLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "blks_hit",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "database", "blks_hit_total"),
					"Total number of times disk blocks were found already in the buffer cache, so that a read was not necessary.",
					databaseLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "tup_returned",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "database", "tup_returned_total"),
					"Total number of rows returned by queries in this database.",
					databaseLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "tup_fetched",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "database", "tup_fetched_total"),
					"Total number of rows fetched by queries in this database.",
					databaseLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "tup_inserted",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "database", "tup_inserted_total"),
					"Total number of rows inserted by queries in this database.",
					databaseLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "tup_updated",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "database", "tup_updated_total"),
					"Total number of rows updated by queries in this database.",
					databaseLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "tup_deleted",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "database", "tup_deleted_total"),
					"Total number of rows deleted by queries in this database.",
					databaseLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "conflicts",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "database", "conflicts_total"),
					"Number of queries canceled due to conflicts with recovery in this database.",
					databaseLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "temp_files",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "database", "temp_files_total"),
					"Number of temporary files created by queries in this database.",
					databaseLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "temp_bytes",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "database", "temp_bytes_total"),
					"Total amount of data written to temporary files by queries in this database.",
					databaseLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "deadlocks",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "database", "deadlocks_total"),
					"Number of deadlocks detected in this database.",
					databaseLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "blk_read_time",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "database", "blk_read_time_seconds"),
					"Time spent reading data file blocks by backends in this database, in seconds.",
					databaseLabelNames, constLabels,
				), valueType: prometheus.CounterValue, factor: .001,
			},
			{
				colname: "blk_write_time",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "database", "blk_write_time_seconds"),
					"Time spent writing data file blocks by backends in this database, in seconds.",
					databaseLabelNames, constLabels,
				), valueType: prometheus.CounterValue, factor: .001,
			},
			{
				colname: "size_bytes",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "database", "size_bytes_total"),
					"Total size of the database, in bytes.",
					databaseLabelNames, constLabels,
				), valueType: prometheus.GaugeValue,
			},
			{
				colname: "stats_age_seconds",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "database", "stats_age_seconds"),
					"The age of the activity statistics, in seconds.",
					databaseLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
		},
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresDatabasesCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.NewDB(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	res, err := conn.GetStats(databaseQuery)
	if err != nil {
		return err
	}

	return parsePostgresStats(res, ch, c.descs, c.labelNames)
}
