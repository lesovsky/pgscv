package collector

import (
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

const poolQuery = "SHOW POOLS"

type pgbouncerPoolsCollector struct {
	descs      []typedDesc
	labelNames []string
}

func NewPgbouncerPoolsCollector(constLabels prometheus.Labels) (Collector, error) {
	var poolsLabelNames = []string{"database", "user", "pool_mode"}

	return &pgbouncerPoolsCollector{
		labelNames: poolsLabelNames,
		descs: []typedDesc{
			{
				colname: "cl_active",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "pgbouncer", "pool_cl_active_total"),
					"The total number of active clients connected.",
					poolsLabelNames, constLabels,
				), valueType: prometheus.GaugeValue,
			},
			{
				colname: "cl_waiting",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgscv", "pgbouncer", "pool_cl_waiting_total"),
					"The total number of waiting clients connected.",
					poolsLabelNames, constLabels,
				), valueType: prometheus.GaugeValue,
			},
		},
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *pgbouncerPoolsCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.NewDB(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	res, err := conn.GetStats(poolQuery)
	if err != nil {
		return err
	}

	return parseStats(res, ch, c.descs, c.labelNames)
}
