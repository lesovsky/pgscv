package collector

import (
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/lesovsky/pgscv/internal/log"
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/lesovsky/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"strings"
)

const (
	// postgresStatementsQuery12 defines query for querying statements metrics for PG12 and older.
	postgresStatementsQuery12 = "SELECT d.datname AS database, pg_get_userbyid(p.userid) AS user, p.queryid, " +
		"p.query, p.calls, p.rows, p.total_time, p.blk_read_time, p.blk_write_time, " +
		"nullif(p.shared_blks_hit, 0) AS shared_blks_hit, nullif(p.shared_blks_read, 0) AS shared_blks_read, " +
		"nullif(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, nullif(p.shared_blks_written, 0) AS shared_blks_written, " +
		"nullif(p.local_blks_hit, 0) AS local_blks_hit, nullif(p.local_blks_read, 0) AS local_blks_read, " +
		"nullif(p.local_blks_dirtied, 0) AS local_blks_dirtied, nullif(p.local_blks_written, 0) AS local_blks_written, " +
		"nullif(p.temp_blks_read, 0) AS temp_blks_read, nullif(p.temp_blks_written, 0) AS temp_blks_written " +
		"FROM %s.pg_stat_statements p JOIN pg_database d ON d.oid=p.dbid"

	// postgresStatementsQueryLatest defines query for querying statements metrics.
	// 1. use nullif(value, 0) to nullify zero values, NULL are skipped by stats method and metrics wil not be generated.
	postgresStatementsQueryLatest = "SELECT d.datname AS database, pg_get_userbyid(p.userid) AS user, p.queryid, " +
		"p.query, p.calls, p.rows, p.total_exec_time, p.total_plan_time, p.blk_read_time, p.blk_write_time, " +
		"nullif(p.shared_blks_hit, 0) AS shared_blks_hit, nullif(p.shared_blks_read, 0) AS shared_blks_read, " +
		"nullif(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, nullif(p.shared_blks_written, 0) AS shared_blks_written, " +
		"nullif(p.local_blks_hit, 0) AS local_blks_hit, nullif(p.local_blks_read, 0) AS local_blks_read, " +
		"nullif(p.local_blks_dirtied, 0) AS local_blks_dirtied, nullif(p.local_blks_written, 0) AS local_blks_written, " +
		"nullif(p.temp_blks_read, 0) AS temp_blks_read, nullif(p.temp_blks_written, 0) AS temp_blks_written, " +
		"nullif(p.wal_records, 0) AS wal_records, nullif(p.wal_fpi, 0) AS wal_fpi, nullif(p.wal_bytes, 0) AS wal_bytes " +
		"FROM %s.pg_stat_statements p JOIN pg_database d ON d.oid=p.dbid"
)

// postgresStatementsCollector ...
type postgresStatementsCollector struct {
	query         typedDesc
	calls         typedDesc
	rows          typedDesc
	times         typedDesc
	allTimes      typedDesc
	sharedHit     typedDesc
	sharedRead    typedDesc
	sharedDirtied typedDesc
	sharedWritten typedDesc
	localHit      typedDesc
	localRead     typedDesc
	localDirtied  typedDesc
	localWritten  typedDesc
	tempRead      typedDesc
	tempWritten   typedDesc
	walRecords    typedDesc
	walFPI        typedDesc
	walBytes      typedDesc
}

// NewPostgresStatementsCollector returns a new Collector exposing postgres statements stats.
// For details see https://www.postgresql.org/docs/current/pgstatstatements.html
func NewPostgresStatementsCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &postgresStatementsCollector{
		query: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "query_info", "Labeled info about statements has been executed.", 0},
			prometheus.GaugeValue,
			[]string{"user", "database", "queryid", "query"}, constLabels,
			settings.Filters,
		),
		calls: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "calls_total", "Total number of times statement has been executed.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		rows: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "rows_total", "Total number of rows retrieved or affected by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		times: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "time_seconds_total", "Time spent by the statement in each mode, in seconds.", .001},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid", "mode"}, constLabels,
			settings.Filters,
		),
		allTimes: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "time_seconds_all_total", "Total time spent by the statement, in seconds.", .001},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		sharedHit: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "shared_hit_bytes_total", "Total number of bytes found in shared buffers by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		sharedRead: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "shared_read_bytes_total", "Total number of bytes read from disk or OS page cache when reading from shared buffers by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		sharedDirtied: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "shared_dirtied_bytes_total", "Total number of bytes dirtied in shared buffers by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		sharedWritten: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "shared_written_bytes_total", "Total number of bytes written to shared buffers by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		localHit: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "local_hit_bytes_total", "Total number of bytes found in local buffers by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		localRead: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "local_read_bytes_total", "Total number of bytes read from disk or OS page cache when reading from local buffers by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		localDirtied: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "local_dirtied_bytes_total", "Total number of bytes dirtied in local buffers by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		localWritten: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "local_written_bytes_total", "Total number of bytes written to local buffers by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		tempRead: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "temp_read_bytes_total", "Total number of bytes read from temporary files by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		tempWritten: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "temp_written_bytes_total", "Total number of bytes written to temporary files by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		walRecords: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "wal_records_total", "Total number of WAL records generated by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		walFPI: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "wal_fpi_bytes_total", "Total number of WAL full-page images generated by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		walBytes: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "wal_bytes_total", "Total number of WAL bytes (not including FPI) generated by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresStatementsCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	// nothing to do, pg_stat_statements not found in shared_preload_libraries
	if !config.pgStatStatements {
		return nil
	}

	// pg_stat_statements could be installed in any database. The database with
	// installed pg_stat_statements is discovered during initial config and stored
	// in configuration. Create the new connection config using default connection
	// string, but replace database with installed pg_stat_statements.

	pgconfig, err := pgx.ParseConfig(config.ConnString)
	if err != nil {
		return err
	}

	pgconfig.Database = config.pgStatStatementsDatabase

	conn, err := store.NewWithConfig(pgconfig)
	if err != nil {
		return err
	}

	defer conn.Close()

	// get pg_stat_statements stats
	res, err := conn.Query(selectStatementsQuery(config.serverVersionNum, config.pgStatStatementsSchema))
	if err != nil {
		return err
	}

	// parse pg_stat_statements stats
	stats := parsePostgresStatementsStats(res, []string{"user", "database", "queryid", "query"})

	blockSize := float64(config.blockSize)

	for _, stat := range stats {
		var query string
		if config.NoTrackMode {
			query = stat.queryid + " /* queryid only, no-track mode enabled */"
		} else {
			query = stat.query
		}

		// Note: pg_stat_statements.total_exec_time (and .total_time) includes blk_read_time and blk_write_time implicitly.
		// Remember that when creating metrics.

		ch <- c.query.newConstMetric(1, stat.user, stat.database, stat.queryid, query)

		ch <- c.calls.newConstMetric(stat.calls, stat.user, stat.database, stat.queryid)
		ch <- c.rows.newConstMetric(stat.rows, stat.user, stat.database, stat.queryid)

		// total = planning + execution; execution already includes io time.
		ch <- c.allTimes.newConstMetric(stat.totalPlanTime+stat.totalExecTime, stat.user, stat.database, stat.queryid)
		ch <- c.times.newConstMetric(stat.totalPlanTime, stat.user, stat.database, stat.queryid, "planning")

		// execution time = execution - io times.
		ch <- c.times.newConstMetric(stat.totalExecTime-(stat.blkReadTime+stat.blkWriteTime), stat.user, stat.database, stat.queryid, "executing")

		// avoid metrics spamming and send metrics only if they greater than zero.
		if stat.blkReadTime > 0 {
			ch <- c.times.newConstMetric(stat.blkReadTime, stat.user, stat.database, stat.queryid, "ioread")
		}
		if stat.blkWriteTime > 0 {
			ch <- c.times.newConstMetric(stat.blkWriteTime, stat.user, stat.database, stat.queryid, "iowrite")
		}
		if stat.sharedBlksHit > 0 {
			ch <- c.sharedHit.newConstMetric(stat.sharedBlksHit*blockSize, stat.user, stat.database, stat.queryid)
		}
		if stat.sharedBlksRead > 0 {
			ch <- c.sharedRead.newConstMetric(stat.sharedBlksRead*blockSize, stat.user, stat.database, stat.queryid)
		}
		if stat.sharedBlksDirtied > 0 {
			ch <- c.sharedDirtied.newConstMetric(stat.sharedBlksDirtied*blockSize, stat.user, stat.database, stat.queryid)
		}
		if stat.sharedBlksWritten > 0 {
			ch <- c.sharedWritten.newConstMetric(stat.sharedBlksWritten*blockSize, stat.user, stat.database, stat.queryid)
		}
		if stat.localBlksHit > 0 {
			ch <- c.localHit.newConstMetric(stat.localBlksHit*blockSize, stat.user, stat.database, stat.queryid)
		}
		if stat.localBlksRead > 0 {
			ch <- c.localRead.newConstMetric(stat.localBlksRead*blockSize, stat.user, stat.database, stat.queryid)
		}
		if stat.localBlksDirtied > 0 {
			ch <- c.localDirtied.newConstMetric(stat.localBlksDirtied*blockSize, stat.user, stat.database, stat.queryid)
		}
		if stat.localBlksWritten > 0 {
			ch <- c.localWritten.newConstMetric(stat.localBlksWritten*blockSize, stat.user, stat.database, stat.queryid)
		}
		if stat.tempBlksRead > 0 {
			ch <- c.tempRead.newConstMetric(stat.tempBlksRead*blockSize, stat.user, stat.database, stat.queryid)
		}
		if stat.tempBlksWritten > 0 {
			ch <- c.tempWritten.newConstMetric(stat.tempBlksWritten*blockSize, stat.user, stat.database, stat.queryid)
		}
		if stat.walRecords > 0 {
			ch <- c.walRecords.newConstMetric(stat.walRecords, stat.user, stat.database, stat.queryid)
		}
		if stat.walFPI > 0 {
			ch <- c.walFPI.newConstMetric(stat.walFPI*blockSize, stat.user, stat.database, stat.queryid)
		}
		if stat.walBytes > 0 {
			ch <- c.walBytes.newConstMetric(stat.walBytes, stat.user, stat.database, stat.queryid)
		}
	}

	return nil
}

// postgresStatementsStat represents stats values for single statement based on pg_stat_statements.
type postgresStatementStat struct {
	database          string
	user              string
	queryid           string
	query             string
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
	walRecords        float64
	walFPI            float64
	walBytes          float64
}

// parsePostgresStatementsStats parses PGResult and return structs with stats values.
func parsePostgresStatementsStats(r *model.PGResult, labelNames []string) map[string]postgresStatementStat {
	log.Debug("parse postgres statements stats")

	var stats = make(map[string]postgresStatementStat)

	// process row by row - on every row construct 'statement' using database/user/queryHash trio. Next process other row's
	// fields and collect stats for constructed 'statement'.
	for _, row := range r.Rows {
		var database, user, queryid, query string

		// collect label values
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "database":
				database = row[i].String
			case "user":
				user = row[i].String
			case "queryid":
				queryid = row[i].String
			case "query":
				query = row[i].String
			}
		}

		// Create a statement name consisting of trio database/user/queryHash
		statement := strings.Join([]string{database, user, queryid}, "/")

		// Put stats with labels (but with no data values yet) into stats store.
		if _, ok := stats[statement]; !ok {
			stats[statement] = postgresStatementStat{database: database, user: user, queryid: queryid, query: query}
		}

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

			s := stats[statement]

			// Run column-specific logic
			switch string(colname.Name) {
			case "calls":
				s.calls += v
			case "rows":
				s.rows += v
			case "total_time", "total_exec_time":
				s.totalExecTime += v
			case "total_plan_time":
				s.totalPlanTime += v
			case "blk_read_time":
				s.blkReadTime += v
			case "blk_write_time":
				s.blkWriteTime += v
			case "shared_blks_hit":
				s.sharedBlksHit += v
			case "shared_blks_read":
				s.sharedBlksRead += v
			case "shared_blks_dirtied":
				s.sharedBlksDirtied += v
			case "shared_blks_written":
				s.sharedBlksWritten += v
			case "local_blks_hit":
				s.localBlksHit += v
			case "local_blks_read":
				s.localBlksRead += v
			case "local_blks_dirtied":
				s.localBlksDirtied += v
			case "local_blks_written":
				s.localBlksWritten += v
			case "temp_blks_read":
				s.tempBlksRead += v
			case "temp_blks_written":
				s.tempBlksWritten += v
			case "wal_records":
				s.walRecords += v
			case "wal_fpi":
				s.walFPI += v
			case "wal_bytes":
				s.walBytes += v
			default:
				continue
			}

			stats[statement] = s
		}
	}

	return stats
}

// selectStatementsQuery returns suitable statements query depending on passed version.
func selectStatementsQuery(version int, schema string) string {
	switch {
	case version < PostgresV13:
		return fmt.Sprintf(postgresStatementsQuery12, schema)
	default:
		return fmt.Sprintf(postgresStatementsQueryLatest, schema)
	}
}
