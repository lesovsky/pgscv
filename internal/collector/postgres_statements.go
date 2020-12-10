package collector

import (
	"crypto/md5" // #nosec G501
	"fmt"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
	"regexp"
	"strconv"
	"strings"
)

const (
	// postgresStatementsQuery12 defines query for querying statements metrics for PG12 and older.
	postgresStatementsQuery12 = `SELECT
    d.datname AS datname, pg_get_userbyid(p.userid) AS usename,
    p.queryid, regexp_replace(p.query,E'\\s+', ' ', 'g') AS query,
    p.calls, p.rows,
    p.total_time, p.blk_read_time, p.blk_write_time,
    nullif(p.shared_blks_hit, 0) AS shared_blks_hit, nullif(p.shared_blks_read, 0) AS shared_blks_read,
    nullif(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, nullif(p.shared_blks_written, 0) AS shared_blks_written,
    nullif(p.local_blks_hit, 0) AS local_blks_hit, nullif(p.local_blks_read, 0) AS local_blks_read,
    nullif(p.local_blks_dirtied, 0) AS local_blks_dirtied, nullif(p.local_blks_written, 0) AS local_blks_written,
    nullif(p.temp_blks_read, 0) AS temp_blks_read, nullif(p.temp_blks_written, 0) AS temp_blks_written
FROM pg_stat_statements p
JOIN pg_database d ON d.oid=p.dbid`

	// postgresStatementsQueryLatest defines query for querying statements metrics.
	// 1. use nullif(value, 0) to nullify zero values, NULL are skipped by stats method and metrics wil not be generated.
	postgresStatementsQueryLatest = `SELECT
    d.datname AS datname, pg_get_userbyid(p.userid) AS usename,
    p.queryid, regexp_replace(p.query,E'\\s+', ' ', 'g') AS query,
    p.calls, p.rows,
    p.total_exec_time, p.total_plan_time, p.blk_read_time, p.blk_write_time,
    nullif(p.shared_blks_hit, 0) AS shared_blks_hit, nullif(p.shared_blks_read, 0) AS shared_blks_read,
    nullif(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, nullif(p.shared_blks_written, 0) AS shared_blks_written,
    nullif(p.local_blks_hit, 0) AS local_blks_hit, nullif(p.local_blks_read, 0) AS local_blks_read,
    nullif(p.local_blks_dirtied, 0) AS local_blks_dirtied, nullif(p.local_blks_written, 0) AS local_blks_written,
    nullif(p.temp_blks_read, 0) AS temp_blks_read, nullif(p.temp_blks_written, 0) AS temp_blks_written
FROM pg_stat_statements p
JOIN pg_database d ON d.oid=p.dbid`
)

// postgresStatementsCollector ...
type postgresStatementsCollector struct {
	calls    typedDesc
	rows     typedDesc
	times    typedDesc
	allTimes typedDesc
	blocks   typedDesc
}

// NewPostgresStatementsCollector returns a new Collector exposing postgres statements stats.
// For details see https://www.postgresql.org/docs/current/pgstatstatements.html
func NewPostgresStatementsCollector(constLabels prometheus.Labels) (Collector, error) {
	return &postgresStatementsCollector{
		calls: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "statements", "calls_total"),
				"Total number of times statement has been executed.",
				[]string{"usename", "datname", "md5", "query"}, constLabels,
			),
			valueType: prometheus.CounterValue,
		},
		rows: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "statements", "rows_total"),
				"Total number of rows retrieved or affected by the statement.",
				[]string{"usename", "datname", "md5"}, constLabels,
			),
			valueType: prometheus.CounterValue,
		},
		times: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "statements", "time_seconds_total"),
				"Time spent by the statement in each mode, in seconds.",
				[]string{"usename", "datname", "md5", "mode"}, constLabels,
			), valueType: prometheus.CounterValue, factor: .001,
		},
		allTimes: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "statements", "time_all_seconds_total"),
				"Total time spent by the statement, in seconds.",
				[]string{"usename", "datname", "md5"}, constLabels,
			), valueType: prometheus.CounterValue, factor: .001,
		},
		blocks: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "statements", "blocks_total"),
				"Number of blocks processed by the statement in each mode.",
				[]string{"usename", "datname", "md5", "type", "access"}, constLabels,
			),
			valueType: prometheus.CounterValue,
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
	res, err := conn.Query(selectStatementsQuery(config.ServerVersionNum))
	if err != nil {
		return err
	}

	conn.Close()

	// parse pg_stat_statements stats
	stats := parsePostgresStatementsStats(res, []string{"usename", "datname", "queryid", "query"})

	for _, stat := range stats {
		var query string
		if config.NoTrackMode {
			query = stat.queryid + " /* queryid only, no-track mode enabled */"
		} else {
			query = stat.query
		}

		// Note: pg_stat_statements.total_exec_time (and .total_time) includes blk_read_time and blk_write_time implicitly.
		// Remember that when creating metrics.

		ch <- c.calls.mustNewConstMetric(stat.calls, stat.usename, stat.datname, stat.md5hash, query)
		ch <- c.rows.mustNewConstMetric(stat.rows, stat.usename, stat.datname, stat.md5hash)

		// total = planning + execution; execution already includes io time.
		ch <- c.allTimes.mustNewConstMetric(stat.totalPlanTime+stat.totalExecTime, stat.usename, stat.datname, stat.md5hash)
		ch <- c.times.mustNewConstMetric(stat.totalPlanTime, stat.usename, stat.datname, stat.md5hash, "planning")

		// execution time = execution - io times.
		ch <- c.times.mustNewConstMetric(stat.totalExecTime-(stat.blkReadTime+stat.blkWriteTime), stat.usename, stat.datname, stat.md5hash, "executing")

		// avoid metrics spamming and send metrics only if they greater than zero.
		if stat.blkReadTime > 0 {
			ch <- c.times.mustNewConstMetric(stat.blkReadTime, stat.usename, stat.datname, stat.md5hash, "ioread")
		}
		if stat.blkWriteTime > 0 {
			ch <- c.times.mustNewConstMetric(stat.blkWriteTime, stat.usename, stat.datname, stat.md5hash, "iowrite")
		}
		if stat.sharedBlksHit > 0 {
			ch <- c.blocks.mustNewConstMetric(stat.sharedBlksHit, stat.usename, stat.datname, stat.md5hash, "shared", "hit")
		}
		if stat.sharedBlksRead > 0 {
			ch <- c.blocks.mustNewConstMetric(stat.sharedBlksRead, stat.usename, stat.datname, stat.md5hash, "shared", "read")
		}
		if stat.sharedBlksDirtied > 0 {
			ch <- c.blocks.mustNewConstMetric(stat.sharedBlksDirtied, stat.usename, stat.datname, stat.md5hash, "shared", "dirtied")
		}
		if stat.sharedBlksWritten > 0 {
			ch <- c.blocks.mustNewConstMetric(stat.sharedBlksWritten, stat.usename, stat.datname, stat.md5hash, "shared", "written")
		}
		if stat.localBlksHit > 0 {
			ch <- c.blocks.mustNewConstMetric(stat.localBlksHit, stat.usename, stat.datname, stat.md5hash, "local", "hit")
		}
		if stat.localBlksRead > 0 {
			ch <- c.blocks.mustNewConstMetric(stat.localBlksRead, stat.usename, stat.datname, stat.md5hash, "local", "read")
		}
		if stat.localBlksDirtied > 0 {
			ch <- c.blocks.mustNewConstMetric(stat.localBlksDirtied, stat.usename, stat.datname, stat.md5hash, "local", "dirtied")
		}
		if stat.localBlksWritten > 0 {
			ch <- c.blocks.mustNewConstMetric(stat.localBlksWritten, stat.usename, stat.datname, stat.md5hash, "local", "written")
		}
		if stat.tempBlksRead > 0 {
			ch <- c.blocks.mustNewConstMetric(stat.tempBlksRead, stat.usename, stat.datname, stat.md5hash, "temp", "read")
		}
		if stat.tempBlksWritten > 0 {
			ch <- c.blocks.mustNewConstMetric(stat.tempBlksWritten, stat.usename, stat.datname, stat.md5hash, "temp", "written")
		}
	}

	return nil
}

// postgresStatementsStat represents stats values for single statement based on pg_stat_statements.
type postgresStatementStat struct {
	datname           string
	usename           string
	queryid           string
	query             string
	md5hash           string
	calls             float64
	rows              float64
	totalExecTime     float64
	totalPlanTime     float64
	blkReadTime       float64
	blkWriteTime      float64
	sharedBlksHit     float64
	sharedBlksRead    float64
	sharedBlksDirtied float64
	sharedBlksWritten float64
	localBlksHit      float64
	localBlksRead     float64
	localBlksDirtied  float64
	localBlksWritten  float64
	tempBlksRead      float64
	tempBlksWritten   float64
}

// parsePostgresStatementsStats parses PGResult and return structs with stats values.
func parsePostgresStatementsStats(r *model.PGResult, labelNames []string) map[string]postgresStatementStat {
	var stats = make(map[string]postgresStatementStat)

	// process row by row - on every row construct 'statement' using datname/usename/queryHash trio. Next process other row's
	// fields and collect stats for constructed 'statement'.
	for _, row := range r.Rows {
		var datname, usename, queryid, query, md5hash string

		// collect label values
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "datname":
				datname = row[i].String
			case "usename":
				usename = row[i].String
			case "queryid":
				queryid = row[i].String
			case "query":
				query = normalizeStatement(row[i].String)
				md5hash = fmt.Sprintf("%x", md5.Sum([]byte(query))) // #nosec G401
			}
		}

		// Create a statement name consisting of trio database/user/queryHash
		statement := strings.Join([]string{datname, usename, md5hash}, "/")

		// Put stats with labels (but with no data values yet) into stats store.
		if _, ok := stats[statement]; !ok {
			stats[statement] = postgresStatementStat{datname: datname, usename: usename, queryid: queryid, query: query, md5hash: md5hash}
		}

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
				s.calls += v
				stats[statement] = s
			case "rows":
				s := stats[statement]
				s.rows += v
				stats[statement] = s
			case "total_time", "total_exec_time":
				s := stats[statement]
				s.totalExecTime += v
				stats[statement] = s
			case "total_plan_time":
				s := stats[statement]
				s.totalPlanTime += v
				stats[statement] = s
			case "blk_read_time":
				s := stats[statement]
				s.blkReadTime += v
				stats[statement] = s
			case "blk_write_time":
				s := stats[statement]
				s.blkWriteTime += v
				stats[statement] = s
			case "shared_blks_hit":
				s := stats[statement]
				s.sharedBlksHit += v
				stats[statement] = s
			case "shared_blks_read":
				s := stats[statement]
				s.sharedBlksRead += v
				stats[statement] = s
			case "shared_blks_dirtied":
				s := stats[statement]
				s.sharedBlksDirtied += v
				stats[statement] = s
			case "shared_blks_written":
				s := stats[statement]
				s.sharedBlksWritten += v
				stats[statement] = s
			case "local_blks_hit":
				s := stats[statement]
				s.localBlksHit += v
				stats[statement] = s
			case "local_blks_read":
				s := stats[statement]
				s.localBlksRead += v
				stats[statement] = s
			case "local_blks_dirtied":
				s := stats[statement]
				s.localBlksDirtied += v
				stats[statement] = s
			case "local_blks_written":
				s := stats[statement]
				s.localBlksWritten += v
				stats[statement] = s
			case "temp_blks_read":
				s := stats[statement]
				s.tempBlksRead += v
				stats[statement] = s
			case "temp_blks_written":
				s := stats[statement]
				s.tempBlksWritten += v
				stats[statement] = s
			default:
				log.Debugf("unsupported pg_stat_statements stat column: %s, skip", string(colname.Name))
				continue
			}
		}
	}

	return stats
}

// normalizeStatement used for normalize queries and truncates redundant elements like params and values.
func normalizeStatement(stmt string) string {
	re := regexp.MustCompile(`(//.*$|/\*.*?\*/)`) // looking for comment sequences, like '/* ... */ or starting from //.
	stmt = re.ReplaceAllString(stmt, "")

	re = regexp.MustCompile(`(?i)\s+VALUES\s*\(((.\S+),\s?)+(.+?)\)`) // looking for 'VALUES ($1, $2, ..., $123)' sequences.
	stmt = re.ReplaceAllString(stmt, " VALUES (?)")

	re = regexp.MustCompile(`(?i)\s+IN\s*\(((.\S+),\s?)+(.+?)\)`) // looking for 'IN ($1, $2, ..., $123)' sequences.
	stmt = re.ReplaceAllString(stmt, " IN (?)")

	re = regexp.MustCompile(`\(([$\d,\s]+)\)`) // looking for standalone digits.
	stmt = re.ReplaceAllString(stmt, "?")

	re = regexp.MustCompile(`'.+?'`) // looking for standalone quoted values, like 'whatever'.
	stmt = re.ReplaceAllString(stmt, "'?'")

	re = regexp.MustCompile(`(?i)(^SET .+(=|TO))(.+)`) // looking for SET commands.
	stmt = re.ReplaceAllString(stmt, "SET ? TO ?")

	re = regexp.MustCompile(`_(\d|_)+`) // looking for digits starting with underscore, like '_2020' or '_2020_10'.
	stmt = re.ReplaceAllString(stmt, "_?")

	re = regexp.MustCompile(`\$?\b\d+\b`) // looking for standalone digits, like '10'.
	stmt = re.ReplaceAllString(stmt, "?")

	re = regexp.MustCompile(`\s{2,}`) // looking for repeating spaces.
	stmt = re.ReplaceAllString(stmt, " ")

	if len(stmt) > 1000 {
		stmt = stmt[0:1000] + "..."
	}

	return strings.TrimSpace(stmt)
}

// NewDBWithPgStatStatements returns connection to the database where pg_stat_statements available for getting stats.
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
	conn, err := store.NewWithConfig(pgconfig)
	if err != nil {
		return nil, err
	}

	// Check for pg_stat_statements.
	if isExtensionAvailable(conn, "pg_stat_statements") {
		// Set up pg_stat_statements source. It's unnecessary here, because it's already set on previous execution of that
		// function in pessimistic case, but do it explicitly.
		config.PgStatStatementsSource = conn.Conn().Config().Database
		return conn, nil
	}

	// Pessimistic case.
	// If we're here it means pg_stat_statements is not available and we have to walk through all database and looking for it.

	// Drop pg_stat_statements source.
	config.PgStatStatementsSource = ""

	// Get databases list from current connection.
	databases, err := listDatabases(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}

	// Close connection to current database, it's not interesting anymore.
	conn.Close()

	// Establish connection to each database in the list and check where pg_stat_statements is installed.
	for _, d := range databases {
		pgconfig.Database = d
		conn, err := store.NewWithConfig(pgconfig)
		if err != nil {
			log.Warnf("failed connect to database: %s; skip", err)
			continue
		}

		// If pg_stat_statements found, update source and return connection.
		if isExtensionAvailable(conn, "pg_stat_statements") {
			config.PgStatStatementsSource = conn.Conn().Config().Database
			return conn, nil
		}

		// Otherwise close connection and go to next database in the list.
		conn.Close()
	}

	// No luck, if we are here it means all database checked and pg_stat_statements is not found (not installed?)
	return nil, fmt.Errorf("pg_stat_statements not found")
}

// selectStatementsQuery returns suitable statements query depending on passed version.
func selectStatementsQuery(version int) string {
	switch {
	case version < PostgresV13:
		return postgresStatementsQuery12
	default:
		return postgresStatementsQueryLatest
	}
}
