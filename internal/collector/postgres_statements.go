package collector

import (
	"fmt"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"strings"
)

const (
	postgresStatementsQuery = `SELECT
    d.datname AS datname, pg_get_userbyid(p.userid) AS usename,
    p.queryid, left(regexp_replace(p.query,E'\\s+', ' ', 'g'),1024) AS query,
    p.calls
FROM pg_stat_statements p
JOIN pg_database d ON d.oid=p.dbid`

	postgresStatementsQueryNext = `SELECT
    d.datname AS datname, pg_get_userbyid(p.userid) AS usename, p.queryid, left(regexp_replace(p.query,E'\\s+', ' ', 'g'),1024) AS query,
		p.calls, p.rows,
		p.total_time, p.blk_read_time, p.blk_write_time,
    p.shared_blks_hit, p.shared_blks_read, p.shared_blks_dirtied, p.shared_blks_written,
    p.local_blks_hit, p.local_blks_read, p.local_blks_dirtied, p.local_blks_written,
		p.temp_blks_read, p.temp_blks_written
FROM pg_stat_statements p
JOIN pg_database d ON d.oid=p.dbid`
)

// postgresStatementsCollector ...
type postgresStatementsCollector struct {
	labelNames []string
	descs      map[string]typedDesc
}

// NewPostgresStatementsCollector returns a new Collector exposing postgres statements stats.
// For details see https://www.postgresql.org/docs/current/pgstatstatements.html
func NewPostgresStatementsCollector(constLabels prometheus.Labels) (Collector, error) {
	var labelNames = []string{"usename", "datname", "queryid", "query"}

	return &postgresStatementsCollector{
		labelNames: labelNames,
		descs: map[string]typedDesc{
			"calls": {
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "statements", "calls"),
					"Total number of times query has been executed.",
					labelNames, constLabels,
				), valueType: prometheus.CounterValue,
			},
		},
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresStatementsCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	// nothing to do, pg_stat_statements not found in shared_preload_libraries
	if !config.PgStatStatements {
		return nil
	}

	// looking for source database where pg_stat_statements is installed
	conn, err := NewDBWithPgStatStatements(&config)
	if err != nil {
		return err
	}

	// get pg_stat_statements stats
	res, err := conn.GetStats(postgresStatementsQuery)
	if err != nil {
		return err
	}

	conn.Close()

	// parse pg_stat_statements stats
	stats := parsePostgresStatementsStats(res, c.labelNames)

	for _, stat := range stats {
		for name, desc := range c.descs {
			switch name {
			case "calls":
				ch <- desc.mustNewConstMetric(stat.calls, stat.datname, stat.usename, stat.queryid, stat.query)
			}
		}
	}

	return nil
}

func parsePostgresStatementsStats(r *store.QueryResult, labelNames []string) map[string]postgresStatementsStat {
	var stats = make(map[string]postgresStatementsStat)

	// process row by row - on every row construct 'statement' using datname/usename/queryid trio. Next process other row's
	// fields and collect stats for constructed 'statement'.
	for _, row := range r.Rows {
		stat := postgresStatementsStat{}

		// collect label values
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "datname":
				stat.datname = row[i].String
			case "usename":
				stat.usename = row[i].String
			case "queryid":
				stat.queryid = row[i].String
			case "query":
				stat.query = row[i].String
			}
		}

		// Create a pool name consisting of trio database/user/queryid
		statement := strings.Join([]string{stat.datname, stat.usename, stat.queryid}, "/")

		// Put stats with labels (but with no data values yet) into stats store.
		stats[statement] = stat

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
				s := stats[statement]
				s.calls = v
				stats[statement] = s
			default:
				log.Debugf("unsupported pg_stat_statements stat column: %s, skip", string(colname.Name))
				continue
			}
		}
	}

	return stats
}

// postgresStatementsStat represents stats values for single statement
type postgresStatementsStat struct {
	datname string
	usename string
	queryid string
	query   string
	calls   float64
}

// lNewDBWithPgStatStatements returns connection to the database where pg_stat_statements available for qetting stats.
// Executing this function supposes pg_stat_statements is already available in shared_preload_libraries (checked when
// setting up service).
func NewDBWithPgStatStatements(config *Config) (*store.DB, error) {
	pgconfig, err := pgx.ParseConfig(config.ConnString)
	if err != nil {
		return nil, err
	}

	// Override database name in connection config and use previously found pg_stat_statements source.
	if config.PgStatStatementsSource != "" {
		pgconfig.Database = config.PgStatStatementsSource
	}

	// Establish connection using config.
	conn, err := store.NewDBConfig(pgconfig)
	if err != nil {
		return nil, err
	}

	// Check for pg_stat_statements.
	if conn.IsExtensionAvailable("pg_stat_statements") {
		// Set up pg_stat_statements source. It's unnecessary here, because it's already set on previous execution of that
		// function in pessimistic case, but do it explicitly.
		config.PgStatStatementsSource = conn.Config.Database
		return conn, nil
	}

	// Pessimistic case.
	// If we're here it means pg_stat_statements is not available and we have to walk through all database and looking for it.

	// Drop pg_stat_statements source.
	config.PgStatStatementsSource = ""

	// Get databases list from current connection.
	databases, err := conn.GetDatabases()
	if err != nil {
		conn.Close()
		return nil, err
	}

	// Close connection to current database, it's not interesting anymore.
	conn.Close()

	// Establish connection to each database in the list and check where pg_stat_statements is installed.
	for _, d := range databases {
		pgconfig.Database = d
		conn, err := store.NewDBConfig(pgconfig)
		if err != nil {
			log.Warnf("failed connect to database: %s; skip", err)
			continue
		}

		// If pg_stat_statements found, update source and return connection.
		if conn.IsExtensionAvailable("pg_stat_statements") {
			config.PgStatStatementsSource = conn.Config.Database
			return conn, nil
		}

		// Otherwise close connection and go to next database in the list.
		conn.Close()
	}

	// No luck, if we are here it means all database checked and pg_stat_statements is not found (not installed?)
	return nil, fmt.Errorf("pg_stat_statements not found")
}
