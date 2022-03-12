package collector

import (
	"github.com/lesovsky/pgscv/internal/log"
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/lesovsky/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
)

const pgbouncerStatsQuery = "SHOW STATS"

type pgbouncerStatsCollector struct {
	up         typedDesc
	xacts      typedDesc
	queries    typedDesc
	bytes      typedDesc
	time       typedDesc
	labelNames []string
}

// NewPgbouncerStatsCollector returns a new Collector exposing pgbouncer pools usage stats (except averages).
// For details see https://www.pgbouncer.org/usage.html#show-stats.
func NewPgbouncerStatsCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	var pgbouncerLabelNames = []string{"database"}

	return &pgbouncerStatsCollector{
		labelNames: pgbouncerLabelNames,
		up: newBuiltinTypedDesc(
			descOpts{"pgbouncer", "", "up", "State of Pgbouncer service: 0 is down, 1 is up.", 0},
			prometheus.CounterValue,
			nil, constLabels,
			settings.Filters,
		),
		xacts: newBuiltinTypedDesc(
			descOpts{"pgbouncer", "", "transactions_total", "Total number of SQL transactions processed, for each database.", 0},
			prometheus.CounterValue,
			pgbouncerLabelNames, constLabels,
			settings.Filters,
		),
		queries: newBuiltinTypedDesc(
			descOpts{"pgbouncer", "", "queries_total", "Total number of SQL queries processed, for each database.", 0},
			prometheus.CounterValue,
			pgbouncerLabelNames, constLabels,
			settings.Filters,
		),
		bytes: newBuiltinTypedDesc(
			descOpts{"pgbouncer", "", "bytes_total", "Total volume of network traffic processed by pgbouncer in each direction, in bytes.", 0},
			prometheus.CounterValue,
			[]string{"database", "type"}, constLabels,
			settings.Filters,
		),
		time: newBuiltinTypedDesc(
			descOpts{
				"pgbouncer", "", "spent_seconds_total",
				"Total number of time spent by pgbouncer when connected to PostgreSQL executing queries or processing transactions, in seconds.",
				.000001,
			},
			prometheus.CounterValue,
			[]string{"database", "type", "mode"}, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *pgbouncerStatsCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		ch <- c.up.newConstMetric(0)
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

	// All is ok, collect up metric.
	ch <- c.up.newConstMetric(1)

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

			s := stats[databaseFQName]

			// Run column-specific logic
			switch string(colname.Name) {
			case "total_xact_count":
				s.xacts = v
			case "total_query_count":
				s.queries = v
			case "total_received":
				s.received = v
			case "total_sent":
				s.sent = v
			case "total_xact_time":
				s.xacttime = v
			case "total_query_time":
				s.querytime = v
			case "total_wait_time":
				s.waittime = v
			default:
				continue
			}

			stats[databaseFQName] = s
		}
	}

	return stats
}
