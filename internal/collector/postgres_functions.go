package collector

import (
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
)

const postgresFunctionsQuery = `SELECT current_database() AS datname, schemaname, funcname, calls, total_time, self_time FROM pg_stat_user_functions`

type postgresFunctionsCollector struct {
	descs      []typedDesc
	labelNames []string
}

// NewPostgresFunctionsCollector returns a new Collector exposing postgres SQL functions stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-USER-FUNCTIONS-VIEW
func NewPostgresFunctionsCollector(constLabels prometheus.Labels) (Collector, error) {
	var labelNames = []string{"datname", "schemaname", "funcname"}

	return &postgresFunctionsCollector{
		labelNames: labelNames,
		descs: []typedDesc{
			{
				colname: "calls",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "function", "calls_total"),
					"Total number of times this function has been called.",
					labelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "total_time",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "function", "total_time_seconds"),
					"Total time spent in this function and all other functions called by it, in seconds.",
					labelNames, constLabels,
				), valueType: prometheus.CounterValue, factor: .001,
			},
			{
				colname: "self_time",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "function", "self_time_seconds"),
					"Total time spent in this function itself, not including other functions called by it, in seconds.",
					labelNames, constLabels,
				), valueType: prometheus.CounterValue, factor: .001,
			},
		},
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresFunctionsCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.NewDB(config.ConnString)
	if err != nil {
		return err
	}

	databases, err := conn.GetDatabases()
	if err != nil {
		return err
	}

	conn.Close()

	pgconfig, err := pgx.ParseConfig(config.ConnString)
	if err != nil {
		return err
	}

	for _, d := range databases {
		pgconfig.Database = d
		conn, err := store.NewDBConfig(pgconfig)
		if err != nil {
			return err
		}

		res, err := conn.GetStats(postgresFunctionsQuery)
		if err != nil {
			log.Warnf("failed get functions stat for datname %s: %s", err, d)
			continue
		}

		err = parseStats(res, ch, c.descs, c.labelNames)
		if err != nil {
			log.Warnf("failed parse functions stat for datname %s: %s", err, d)
			continue
		}
	}

	return nil
}
