package collector

import (
	"github.com/jackc/pgx/v4"
	"github.com/lesovsky/pgscv/internal/log"
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/lesovsky/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"strings"
)

const postgresFunctionsQuery = "SELECT current_database() AS database, schemaname AS schema, funcname AS function, calls, total_time, self_time FROM pg_stat_user_functions"

type postgresFunctionsCollector struct {
	calls      typedDesc
	totaltime  typedDesc
	selftime   typedDesc
	labelNames []string
}

// NewPostgresFunctionsCollector returns a new Collector exposing postgres SQL functions stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-USER-FUNCTIONS-VIEW
func NewPostgresFunctionsCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	var labelNames = []string{"database", "schema", "function"}

	return &postgresFunctionsCollector{
		labelNames: labelNames,
		calls: newBuiltinTypedDesc(
			descOpts{"postgres", "function", "calls_total", "Total number of times functions had been called.", 0},
			prometheus.CounterValue,
			labelNames, constLabels,
			settings.Filters,
		),
		totaltime: newBuiltinTypedDesc(
			descOpts{"postgres", "function", "total_time_seconds_total", "Total time spent in function and all other functions called by it, in seconds.", .001},
			prometheus.CounterValue,
			labelNames, constLabels,
			settings.Filters,
		),
		selftime: newBuiltinTypedDesc(
			descOpts{"postgres", "function", "self_time_seconds_total", "Total time spent in function itself, not including other functions called by it, in seconds.", .001},
			prometheus.CounterValue,
			labelNames, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresFunctionsCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}

	databases, err := listDatabases(conn)
	if err != nil {
		return err
	}

	conn.Close()

	pgconfig, err := pgx.ParseConfig(config.ConnString)
	if err != nil {
		return err
	}

	for _, d := range databases {
		// Skip database if not matched to allowed.
		if config.DatabasesRE != nil && !config.DatabasesRE.MatchString(d) {
			continue
		}

		pgconfig.Database = d
		conn, err := store.NewWithConfig(pgconfig)
		if err != nil {
			return err
		}

		res, err := conn.Query(postgresFunctionsQuery)
		conn.Close()
		if err != nil {
			log.Warnf("get functions stat of database %s failed: %s", d, err)
			continue
		}

		stats := parsePostgresFunctionsStats(res, c.labelNames)

		for _, stat := range stats {
			ch <- c.calls.newConstMetric(stat.calls, stat.database, stat.schema, stat.function)
			ch <- c.totaltime.newConstMetric(stat.totaltime, stat.database, stat.schema, stat.function)
			ch <- c.selftime.newConstMetric(stat.selftime, stat.database, stat.schema, stat.function)
		}
	}

	return nil
}

// postgresFunctionStat represents Postgres function stats based pg_stat_user_functions.
type postgresFunctionStat struct {
	database  string
	schema    string
	function  string
	calls     float64
	totaltime float64
	selftime  float64
}

// parsePostgresFunctionsStats parses PGResult and return struct with stats values.
func parsePostgresFunctionsStats(r *model.PGResult, labelNames []string) map[string]postgresFunctionStat {
	log.Debug("parse postgres user functions stats")

	var stats = make(map[string]postgresFunctionStat)

	// process row by row
	for _, row := range r.Rows {
		stat := postgresFunctionStat{}

		// collect label values
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "database":
				stat.database = row[i].String
			case "schema":
				stat.schema = row[i].String
			case "function":
				stat.function = row[i].String
			}
		}

		// Create a function name consisting of trio database/schema/function
		functionFQName := strings.Join([]string{stat.database, stat.schema, stat.function}, "/")

		// Put stats with labels (but with no data values yet) into stats store.
		stats[functionFQName] = stat

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
				log.Errorf("invalid input, parse '%s' failed: %s; skip", row[i].String, err)
				continue
			}

			s := stats[functionFQName]

			// Run column-specific logic
			switch string(colname.Name) {
			case "calls":
				s.calls = v
			case "total_time":
				s.totaltime = v
			case "self_time":
				s.selftime = v
			default:
				continue
			}

			stats[functionFQName] = s
		}
	}

	return stats
}
