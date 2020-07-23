package collector

import (
	"fmt"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

const databaseQuery = "SELECT COALESCE(datname, '__shared__') AS datname, xact_commit, xact_rollback FROM pg_stat_database"

type postgresDatabaseCollector struct {
	descs      []typedDesc
	labelNames []string
}

func NewPostgresDatabaseCollector(labels prometheus.Labels) (Collector, error) {
	var databaseLabelNames = []string{"database"}

	return &postgresDatabaseCollector{
		labelNames: []string{"datname"},
		descs: []typedDesc{
			{
				colname: "xact_commit",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "database", "xact_commit_total"),
					"The total number of transactions committed.",
					databaseLabelNames, labels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "xact_rollback",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "database", "xact_rollback_total"),
					"The total number of transactions rolled back.",
					databaseLabelNames, labels,
				), valueType: prometheus.CounterValue,
			},
		},
	}, nil
}

func (c *postgresDatabaseCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	db, err := store.NewDB(config.ConnString)
	if err != nil {
		return err
	}

	qq, err := getStats(db, databaseQuery)
	if err != nil {
		return err
	}

	return parseStats(qq, ch, c.descs, c.labelNames)
}

func lookupDesc(descs []typedDesc, pattern string) (int, error) {
	for i, desc := range descs {
		if desc.colname == pattern {
			return i, nil
		}
	}
	return -1, fmt.Errorf("pattern not found")
}
