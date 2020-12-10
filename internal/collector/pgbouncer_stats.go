package collector

import (
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
)

const pgbouncerStatsQuery = "SHOW STATS"

type pgbouncerStatsCollector struct {
	xacts      typedDesc
	queries    typedDesc
	bytes      typedDesc
	time       typedDesc
	labelNames []string
}

// NewPgbouncerStatsCollector returns a new Collector exposing pgbouncer pools usage stats (except averages).
// For details see https://www.pgbouncer.org/usage.html#show-stats.
func NewPgbouncerStatsCollector(constLabels prometheus.Labels) (Collector, error) {
	var pgbouncerLabelNames = []string{"database"}

	return &pgbouncerStatsCollector{
		labelNames: pgbouncerLabelNames,
		xacts: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("pgbouncer", "", "transactions_total"),
				"Total number of SQL transactions processed, for each database.",
				pgbouncerLabelNames, constLabels,
			), valueType: prometheus.CounterValue,
		},
		queries: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("pgbouncer", "", "queries_total"),
				"Total number of SQL queries processed, for each database.",
				pgbouncerLabelNames, constLabels,
			), valueType: prometheus.CounterValue,
		},
		bytes: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("pgbouncer", "", "bytes_total"),
				"Total volume of network traffic processed by pgbouncer in each direction, in bytes.",
				[]string{"database", "type"}, constLabels,
			), valueType: prometheus.CounterValue,
		},
		time: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("pgbouncer", "", "spent_seconds_total"),
				"Total number of time spent by pgbouncer when connected to PostgreSQL executing queries or processing transactions, in seconds.",
				[]string{"database", "type", "mode"}, constLabels,
			), valueType: prometheus.CounterValue, factor: .000001,
		},
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *pgbouncerStatsCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	res, err := conn.Query(pgbouncerStatsQuery)
	if err != nil {
		return err
	}

	stats := parsePgbouncerStatsStats(res, c.labelNames)

	for _, stat := range stats {
		ch <- c.xacts.mustNewConstMetric(stat.xacts, stat.database)
		ch <- c.queries.mustNewConstMetric(stat.queries, stat.database)
		ch <- c.bytes.mustNewConstMetric(stat.received, stat.database, "received")
		ch <- c.bytes.mustNewConstMetric(stat.sent, stat.database, "sent")
		ch <- c.time.mustNewConstMetric(stat.xacttime, stat.database, "running", "xact")
		ch <- c.time.mustNewConstMetric(stat.querytime, stat.database, "running", "query")
		ch <- c.time.mustNewConstMetric(stat.waittime, stat.database, "waiting", "none")
	}

	return nil
}

// pgbouncerStatsStat represents general stats provided by 'SHOW STATS' command
type pgbouncerStatsStat struct {
	database  string
	xacts     float64
	queries   float64
	received  float64
	sent      float64
	xacttime  float64
	querytime float64
	waittime  float64
}

// parsePgbouncerStatsStats parses passed PGResult and result struct with data values extracted from PGResult
func parsePgbouncerStatsStats(r *model.PGResult, labelNames []string) map[string]pgbouncerStatsStat {
	var stats = make(map[string]pgbouncerStatsStat)

	// process row by row
	for _, row := range r.Rows {
		stat := pgbouncerStatsStat{}

		// collect label values
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "database":
				stat.database = row[i].String
			}
		}

		// Create map key based on database (pool) name
		databaseFQName := stat.database

		// Put stats with labels (but with no data values yet) into stats store.
		stats[databaseFQName] = stat

		// fetch data values from columns
		for i, colname := range r.Colnames {
			// skip columns if its value used as a label
			if stringsContains(labelNames, string(colname.Name)) {
				log.Debug("skip label mapped column")
				continue
			}

			// Skip empty (NULL) values.
			if row[i].String == "" {
				log.Debug("got empty (NULL) value, skip")
				continue
			}

			// Get data value and convert it to float64 used by Prometheus.
			v, err := strconv.ParseFloat(row[i].String, 64)
			if err != nil {
				log.Errorf("skip collecting metric: %s", err)
				continue
			}

			// Run column-specific logic
			switch string(colname.Name) {
			case "total_xact_count":
				s := stats[databaseFQName]
				s.xacts = v
				stats[databaseFQName] = s
			case "total_query_count":
				s := stats[databaseFQName]
				s.queries = v
				stats[databaseFQName] = s
			case "total_received":
				s := stats[databaseFQName]
				s.received = v
				stats[databaseFQName] = s
			case "total_sent":
				s := stats[databaseFQName]
				s.sent = v
				stats[databaseFQName] = s
			case "total_xact_time":
				s := stats[databaseFQName]
				s.xacttime = v
				stats[databaseFQName] = s
			case "total_query_time":
				s := stats[databaseFQName]
				s.querytime = v
				stats[databaseFQName] = s
			case "total_wait_time":
				s := stats[databaseFQName]
				s.waittime = v
				stats[databaseFQName] = s
			default:
				log.Debugf("unsupported 'SHOW STATS' stat column: %s, skip", string(colname.Name))
				continue
			}
		}
	}

	return stats
}
