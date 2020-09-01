package collector

import (
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"strings"
)

const postgresFunctionsQuery = `SELECT current_database() AS datname, schemaname, funcname, calls, total_time, self_time FROM pg_stat_user_functions`

type postgresFunctionsCollector struct {
	calls      typedDesc
	totaltime  typedDesc
	selftime   typedDesc
	labelNames []string
}

// NewPostgresFunctionsCollector returns a new Collector exposing postgres SQL functions stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-USER-FUNCTIONS-VIEW
func NewPostgresFunctionsCollector(constLabels prometheus.Labels) (Collector, error) {
	var labelNames = []string{"datname", "schemaname", "funcname"}

	return &postgresFunctionsCollector{
		labelNames: labelNames,
		calls: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "function", "calls_total"),
				"Total number of times this function has been called.",
				labelNames, constLabels,
			), valueType: prometheus.CounterValue,
		},
		totaltime: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "function", "total_time_seconds"),
				"Total time spent in this function and all other functions called by it, in seconds.",
				labelNames, constLabels,
			), valueType: prometheus.CounterValue, factor: .001,
		},
		selftime: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "function", "self_time_seconds"),
				"Total time spent in this function itself, not including other functions called by it, in seconds.",
				labelNames, constLabels,
			), valueType: prometheus.CounterValue, factor: .001,
		},
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
			ch <- c.calls.mustNewConstMetric(stat.calls, stat.datname, stat.schemaname, stat.funcname)
			ch <- c.totaltime.mustNewConstMetric(stat.totaltime, stat.datname, stat.schemaname, stat.funcname)
			ch <- c.selftime.mustNewConstMetric(stat.selftime, stat.datname, stat.schemaname, stat.funcname)
		}
	}

	return nil
}

// postgresFunctionStat represents Postgres function stats based pg_stat_user_functions.
type postgresFunctionStat struct {
	datname    string
	schemaname string
	funcname   string
	calls      float64
	totaltime  float64
	selftime   float64
}

// parsePostgresFunctionsStats parses PGResult and return struct with stats values.
func parsePostgresFunctionsStats(r *model.PGResult, labelNames []string) map[string]postgresFunctionStat {
	var stats = make(map[string]postgresFunctionStat)

	// process row by row
	for _, row := range r.Rows {
		stat := postgresFunctionStat{}

		// collect label values
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "datname":
				stat.datname = row[i].String
			case "schemaname":
				stat.schemaname = row[i].String
			case "funcname":
				stat.funcname = row[i].String
			}
		}

		// Create a function name consisting of trio database/user/queryid
		functionFQName := strings.Join([]string{stat.datname, stat.schemaname, stat.funcname}, "/")

		// Put stats with labels (but with no data values yet) into stats store.
		stats[functionFQName] = stat

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
			case "calls":
				s := stats[functionFQName]
				s.calls = v
				stats[functionFQName] = s
			case "total_time":
				s := stats[functionFQName]
				s.totaltime = v
				stats[functionFQName] = s
			case "self_time":
				s := stats[functionFQName]
				s.selftime = v
				stats[functionFQName] = s
			default:
				log.Debugf("unsupported pg_stat_user_functions stat column: %s, skip", string(colname.Name))
				continue
			}
		}
	}

	return stats
}
