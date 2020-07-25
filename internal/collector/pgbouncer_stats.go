package collector

import (
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

const pgbouncerStatsQuery = "SHOW STATS"

type pgbouncerStatsCollector struct {
	descs      []typedDesc
	labelNames []string
}

// NewPgbouncerStatsCollector returns a new Collector exposing pgbouncer pools usage stats (except averages).
// For details see https://www.pgbouncer.org/usage.html#show-stats.
func NewPgbouncerStatsCollector(constLabels prometheus.Labels) (Collector, error) {
	var pgbouncerLabelNames = []string{"database"}

	return &pgbouncerStatsCollector{
		labelNames: pgbouncerLabelNames,
		descs: []typedDesc{
			{
				colname: "total_xact_count",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgbouncer", "", "xact_total"),
					"Total number of SQL transactions pooled by pgbouncer.",
					pgbouncerLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "total_query_count",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgbouncer", "", "query_total"),
					"Total number of SQL queries pooled by pgbouncer.",
					pgbouncerLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "total_received",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgbouncer", "", "received_bytes_total"),
					"Total volume of network traffic received by pgbouncer, in bytes.",
					pgbouncerLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "total_sent",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgbouncer", "", "sent_bytes_total"),
					"Total volume of network traffic sent by pgbouncer, in bytes.",
					pgbouncerLabelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
			{
				colname: "total_xact_time",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgbouncer", "", "xact_time_seconds_total"),
					"Total number of time spent by pgbouncer when connected to PostgreSQL in a transaction, either idle in transaction or executing queries, in seconds.",
					pgbouncerLabelNames, constLabels,
				), valueType: prometheus.CounterValue, factor: .000001,
			},
			{
				colname: "total_query_time",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgbouncer", "", "query_time_seconds_total"),
					"Total number of time spent by pgbouncer when actively connected to PostgreSQL, executing queries, in seconds.",
					pgbouncerLabelNames, constLabels,
				), valueType: prometheus.CounterValue, factor: .000001,
			},
			{
				colname: "total_wait_time",
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("pgbouncer", "", "wait_time_seconds_total"),
					"Time spent by clients waiting for a server, in seconds.",
					pgbouncerLabelNames, constLabels,
				), valueType: prometheus.CounterValue, factor: .000001,
			},
		},
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *pgbouncerStatsCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.NewDB(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	res, err := conn.GetStats(pgbouncerStatsQuery)
	if err != nil {
		return err
	}

	return parseStats(res, ch, c.descs, c.labelNames)
}
