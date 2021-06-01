package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/filter"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
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
func NewPgbouncerStatsCollector(constLabels prometheus.Labels, _ model.CollectorSettings) (Collector, error) {
	var pgbouncerLabelNames = []string{"database"}

	return &pgbouncerStatsCollector{
		labelNames: pgbouncerLabelNames,
		xacts: newBuiltinTypedDesc(
			descOpts{"pgbouncer", "", "transactions_total", "Total number of SQL transactions processed, for each database.", 0},
			prometheus.CounterValue,
			pgbouncerLabelNames, constLabels,
			filter.New(),
		),
		queries: newBuiltinTypedDesc(
			descOpts{"pgbouncer", "", "queries_total", "Total number of SQL queries processed, for each database.", 0},
			prometheus.CounterValue,
			pgbouncerLabelNames, constLabels,
			filter.New(),
		),
		bytes: newBuiltinTypedDesc(
			descOpts{"pgbouncer", "", "bytes_total", "Total volume of network traffic processed by pgbouncer in each direction, in bytes.", 0},
			prometheus.CounterValue,
			[]string{"database", "type"}, constLabels,
			filter.New(),
		),
		time: newBuiltinTypedDesc(
			descOpts{
				"pgbouncer", "", "spent_seconds_total",
				"Total number of time spent by pgbouncer when connected to PostgreSQL executing queries or processing transactions, in seconds.",
				.000001,
			},
			prometheus.CounterValue,
			[]string{"database", "type", "mode"}, constLabels,
			filter.New(),
		),
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
		ch <- c.xacts.newConstMetric(stat.xacts, stat.database)
		ch <- c.queries.newConstMetric(stat.queries, stat.database)
		ch <- c.bytes.newConstMetric(stat.received, stat.database, "received")
		ch <- c.bytes.newConstMetric(stat.sent, stat.database, "sent")
		ch <- c.time.newConstMetric(stat.xacttime, stat.database, "running", "xact")
		ch <- c.time.newConstMetric(stat.querytime, stat.database, "running", "query")
		ch <- c.time.newConstMetric(stat.waittime, stat.database, "waiting", "none")
	}

	return nil
}

// pgbouncerStatsStat represents general stats provided by 'SHOW STATS' command.
// See https://www.pgbouncer.org/usage.html for details.
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
	log.Debug("parse pgbouncer stats")

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
				continue
			}

			// Skip empty (NULL) values.
			if !row[i].Valid {
				continue
			}

			// Get data value and convert it to float64 used by Prometheus.
			v, err := strconv.ParseFloat(row[i].String, 64)
			if err != nil {
				log.Errorf("invalid input, parse '%s' failed: %s; skip", row[i].String, err.Error())
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
				continue
			}
		}
	}

	return stats
}
