package collector

import (
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
)

const userTablesQuery = "SELECT current_database() AS datname, schemaname, relname, seq_scan, seq_tup_read FROM pg_stat_user_tables"

type postgresTablesCollector struct {
	descs      []typedDesc
	labelNames []string
}

func NewPostgresTablesCollector(constLabels prometheus.Labels) (Collector, error) {
	var tablesLabelNames = []string{"datname", "schemaname", "relname"}

	return &postgresTablesCollector{
		labelNames: tablesLabelNames,
		descs: []typedDesc{
			{
				colname: "seq_scan",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "table", "seq_scan_total"),
					"The total number of sequential scans have been done.",
					tablesLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "seq_tup_read",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "table", "seq_tup_read_total"),
					"The total number of tuples have been read by sequential scans.",
					tablesLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
		},
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresTablesCollector) Update(config Config, ch chan<- prometheus.Metric) error {
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

		res, err := conn.GetStats(userTablesQuery)
		if err != nil {
			return err
		}

		if err := parseStats(res, ch, c.descs, c.labelNames); err != nil {
			log.Errorf("failed get tables stats from %s: %s", d, err)
			continue
		}

		conn.Close()
	}

	return nil
}
