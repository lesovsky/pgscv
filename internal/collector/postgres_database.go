package collector

import (
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

const databaseQuery = "SELECT COALESCE(datname, '__shared__') AS datname, xact_commit, xact_rollback FROM pg_stat_database"

type postgresDatabasesCollector struct {
	descs      []typedDesc
	labelNames []string
}

func NewPostgresDatabasesCollector(constLabels prometheus.Labels) (Collector, error) {
	var databaseLabelNames = []string{"datname"}

	return &postgresDatabasesCollector{
		labelNames: databaseLabelNames,
		descs: []typedDesc{
			{
				colname: "xact_commit",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "database", "xact_commit_total"),
					"The total number of transactions committed.",
					databaseLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "xact_rollback",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "database", "xact_rollback_total"),
					"The total number of transactions rolled back.",
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

	return parseStats(res, ch, c.descs, c.labelNames)
}
