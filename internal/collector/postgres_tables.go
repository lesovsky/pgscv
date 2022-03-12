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

const (
	userTablesQuery = "SELECT current_database() AS database, s1.schemaname AS schema, s1.relname AS table, " +
		"seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd, " +
		"n_live_tup, n_dead_tup, n_mod_since_analyze, " +
		"extract('epoch' from age(now(), greatest(last_vacuum, last_autovacuum))) AS last_vacuum_seconds, " +
		"extract('epoch' from age(now(), greatest(last_analyze, last_autoanalyze))) AS last_analyze_seconds, " +
		"extract('epoch' from greatest(last_vacuum, last_autovacuum)) AS last_vacuum_time," +
		"extract('epoch' from greatest(last_analyze, last_autoanalyze)) AS last_analyze_time," +
		"vacuum_count, autovacuum_count, analyze_count, autoanalyze_count, heap_blks_read, heap_blks_hit, idx_blks_read, " +
		"idx_blks_hit, toast_blks_read, toast_blks_hit, tidx_blks_read, tidx_blks_hit, " +
		"pg_table_size(s1.relid) AS size_bytes, reltuples " +
		"FROM pg_stat_user_tables s1 JOIN pg_statio_user_tables s2 USING (schemaname, relname) JOIN pg_class c ON s1.relid = c.oid " +
		"WHERE NOT EXISTS (SELECT 1 FROM pg_locks WHERE relation = s1.relid AND mode = 'AccessExclusiveLock' AND granted)"
)

// postgresTablesCollector defines metric descriptors and stats store.
type postgresTablesCollector struct {
	seqscan              typedDesc
	seqtupread           typedDesc
	idxscan              typedDesc
	idxtupfetch          typedDesc
	tupInserted          typedDesc
	tupUpdated           typedDesc
	tupHotUpdated        typedDesc
	tupDeleted           typedDesc
	tupLive              typedDesc
	tupDead              typedDesc
	tupModified          typedDesc
	maintLastVacuumAge   typedDesc
	maintLastAnalyzeAge  typedDesc
	maintLastVacuumTime  typedDesc
	maintLastAnalyzeTime typedDesc
	maintenance          typedDesc
	io                   typedDesc
	sizes                typedDesc
	reltuples            typedDesc
	labelNames           []string
}

// NewPostgresTablesCollector returns a new Collector exposing postgres tables stats.
// For details see
// https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ALL-TABLES-VIEW
// https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STATIO-ALL-TABLES-VIEW
func NewPostgresTablesCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	var labels = []string{"database", "schema", "table"}

	return &postgresTablesCollector{
		labelNames: labels,
		seqscan: newBuiltinTypedDesc(
			descOpts{"postgres", "table", "seq_scan_total", "The total number of sequential scans have been done.", 0},
			prometheus.CounterValue,
			labels, constLabels,
			settings.Filters,
		),
		seqtupread: newBuiltinTypedDesc(
			descOpts{"postgres", "table", "seq_tup_read_total", "The total number of tuples have been read by sequential scans.", 0},
			prometheus.CounterValue,
			labels, constLabels,
			settings.Filters,
		),
		idxscan: newBuiltinTypedDesc(
			descOpts{"postgres", "table", "idx_scan_total", "Total number of index scans initiated on this table.", 0},
			prometheus.CounterValue,
			labels, constLabels,
			settings.Filters,
		),
		idxtupfetch: newBuiltinTypedDesc(
			descOpts{"postgres", "table", "idx_tup_fetch_total", "Total number of live rows fetched by index scans.", 0},
			prometheus.CounterValue,
			labels, constLabels,
			settings.Filters,
		),
		tupInserted: newBuiltinTypedDesc(
			descOpts{"postgres", "table", "tuples_inserted_total", "Total number of tuples (rows) have been inserted in the table.", 0},
			prometheus.CounterValue,
			labels, constLabels,
			settings.Filters,
		),
		tupUpdated: newBuiltinTypedDesc(
			descOpts{"postgres", "table", "tuples_updated_total", "Total number of tuples (rows) have been updated in the table (including HOT).", 0},
			prometheus.CounterValue,
			labels, constLabels,
			settings.Filters,
		),
		tupHotUpdated: newBuiltinTypedDesc(
			descOpts{"postgres", "table", "tuples_hot_updated_total", "Total number of tuples (rows) have been updated in the table (HOT only).", 0},
			prometheus.CounterValue,
			labels, constLabels,
			settings.Filters,
		),
		tupDeleted: newBuiltinTypedDesc(
			descOpts{"postgres", "table", "tuples_deleted_total", "Total number of tuples (rows) have been deleted in the table.", 0},
			prometheus.CounterValue,
			labels, constLabels,
			settings.Filters,
		),
		tupLive: newBuiltinTypedDesc(
			descOpts{"postgres", "table", "tuples_live_total", "Estimated total number of live tuples in the table.", 0},
			prometheus.GaugeValue,
			labels, constLabels,
			settings.Filters,
		),
		tupDead: newBuiltinTypedDesc(
			descOpts{"postgres", "table", "tuples_dead_total", "Estimated total number of dead tuples in the table.", 0},
			prometheus.GaugeValue,
			labels, constLabels,
			settings.Filters,
		),
		tupModified: newBuiltinTypedDesc(
			descOpts{"postgres", "table", "tuples_modified_total", "Estimated total number of modified tuples in the table since last vacuum.", 0},
			prometheus.GaugeValue,
			labels, constLabels,
			settings.Filters,
		),
		maintLastVacuumAge: newBuiltinTypedDesc(
			descOpts{"postgres", "table", "since_last_vacuum_seconds_total", "Total time since table was vacuumed manually or automatically (not counting VACUUM FULL), in seconds. DEPRECATED.", 0},
			prometheus.CounterValue,
			labels, constLabels,
			settings.Filters,
		),
		maintLastAnalyzeAge: newBuiltinTypedDesc(
			descOpts{"postgres", "table", "since_last_analyze_seconds_total", "Total time since table was analyzed manually or automatically, in seconds. DEPRECATED.", 0},
			prometheus.CounterValue,
			labels, constLabels,
			settings.Filters,
		),
		maintLastVacuumTime: newBuiltinTypedDesc(
			descOpts{"postgres", "table", "last_vacuum_time", "Time of last vacuum or autovacuum has been done (not counting VACUUM FULL), in unixtime.", 0},
			prometheus.CounterValue,
			labels, constLabels,
			settings.Filters,
		),
		maintLastAnalyzeTime: newBuiltinTypedDesc(
			descOpts{"postgres", "table", "last_analyze_time", "Time of last analyze or autoanalyze has been done, in unixtime.", 0},
			prometheus.CounterValue,
			labels, constLabels,
			settings.Filters,
		),
		maintenance: newBuiltinTypedDesc(
			descOpts{"postgres", "table", "maintenance_total", "Total number of times this table has been maintained by each type of maintenance operation.", 0},
			prometheus.CounterValue,
			[]string{"database", "schema", "table", "type"}, constLabels,
			settings.Filters,
		),
		io: newBuiltinTypedDesc(
			descOpts{"postgres", "table_io", "blocks_total", "Total number of table's blocks processed.", 0},
			prometheus.CounterValue,
			[]string{"database", "schema", "table", "type", "access"}, constLabels,
			settings.Filters,
		),
		sizes: newBuiltinTypedDesc(
			descOpts{"postgres", "table", "size_bytes", "Total size of the table (including all forks and TOASTed data), in bytes.", 0},
			prometheus.GaugeValue,
			labels, constLabels,
			settings.Filters,
		),
		reltuples: newBuiltinTypedDesc(
			descOpts{"postgres", "table", "tuples_total", "Number of rows in the table based on pg_class.reltuples value.", 0},
			prometheus.GaugeValue,
			labels, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresTablesCollector) Update(config Config, ch chan<- prometheus.Metric) error {
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

		res, err := conn.Query(userTablesQuery)
		conn.Close()
		if err != nil {
			log.Warnf("get tables stat of database '%s' failed: %s; skip", d, err)
			continue
		}

		stats := parsePostgresTableStats(res, c.labelNames)

		for _, stat := range stats {
			// scan stats
			ch <- c.seqscan.newConstMetric(stat.seqscan, stat.database, stat.schema, stat.table)
			ch <- c.seqtupread.newConstMetric(stat.seqtupread, stat.database, stat.schema, stat.table)
			ch <- c.idxscan.newConstMetric(stat.idxscan, stat.database, stat.schema, stat.table)
			ch <- c.idxtupfetch.newConstMetric(stat.idxtupfetch, stat.database, stat.schema, stat.table)

			// tuples stats
			ch <- c.tupInserted.newConstMetric(stat.inserted, stat.database, stat.schema, stat.table)
			ch <- c.tupUpdated.newConstMetric(stat.updated, stat.database, stat.schema, stat.table)
			ch <- c.tupDeleted.newConstMetric(stat.deleted, stat.database, stat.schema, stat.table)
			ch <- c.tupHotUpdated.newConstMetric(stat.hotUpdated, stat.database, stat.schema, stat.table)

			// tuples total stats
			ch <- c.tupLive.newConstMetric(stat.live, stat.database, stat.schema, stat.table)
			ch <- c.tupDead.newConstMetric(stat.dead, stat.database, stat.schema, stat.table)
			ch <- c.tupModified.newConstMetric(stat.modified, stat.database, stat.schema, stat.table)

			// maintenance stats -- avoid metrics spam produced by inactive tables, don't send metrics if counters are zero.
			if stat.lastvacuumAge > 0 {
				ch <- c.maintLastVacuumAge.newConstMetric(stat.lastvacuumAge, stat.database, stat.schema, stat.table)
			}
			if stat.lastanalyzeAge > 0 {
				ch <- c.maintLastAnalyzeAge.newConstMetric(stat.lastanalyzeAge, stat.database, stat.schema, stat.table)
			}
			if stat.lastvacuumTime > 0 {
				ch <- c.maintLastVacuumTime.newConstMetric(stat.lastvacuumTime, stat.database, stat.schema, stat.table)
			}
			if stat.lastanalyzeTime > 0 {
				ch <- c.maintLastAnalyzeTime.newConstMetric(stat.lastanalyzeTime, stat.database, stat.schema, stat.table)
			}
			if stat.vacuum > 0 {
				ch <- c.maintenance.newConstMetric(stat.vacuum, stat.database, stat.schema, stat.table, "vacuum")
			}
			if stat.autovacuum > 0 {
				ch <- c.maintenance.newConstMetric(stat.autovacuum, stat.database, stat.schema, stat.table, "autovacuum")
			}
			if stat.analyze > 0 {
				ch <- c.maintenance.newConstMetric(stat.analyze, stat.database, stat.schema, stat.table, "analyze")
			}
			if stat.autoanalyze > 0 {
				ch <- c.maintenance.newConstMetric(stat.autoanalyze, stat.database, stat.schema, stat.table, "autoanalyze")
			}

			// io stats -- avoid metrics spam produced by inactive tables, don't send metrics if counters are zero.
			if stat.heapread > 0 {
				ch <- c.io.newConstMetric(stat.heapread, stat.database, stat.schema, stat.table, "heap", "read")
			}
			if stat.heaphit > 0 {
				ch <- c.io.newConstMetric(stat.heaphit, stat.database, stat.schema, stat.table, "heap", "hit")
			}
			if stat.idxread > 0 {
				ch <- c.io.newConstMetric(stat.idxread, stat.database, stat.schema, stat.table, "idx", "read")
			}
			if stat.idxhit > 0 {
				ch <- c.io.newConstMetric(stat.idxhit, stat.database, stat.schema, stat.table, "idx", "hit")
			}
			if stat.toastread > 0 {
				ch <- c.io.newConstMetric(stat.toastread, stat.database, stat.schema, stat.table, "toast", "read")
			}
			if stat.toasthit > 0 {
				ch <- c.io.newConstMetric(stat.toasthit, stat.database, stat.schema, stat.table, "toast", "hit")
			}
			if stat.tidxread > 0 {
				ch <- c.io.newConstMetric(stat.tidxread, stat.database, stat.schema, stat.table, "tidx", "read")
			}
			if stat.tidxhit > 0 {
				ch <- c.io.newConstMetric(stat.tidxhit, stat.database, stat.schema, stat.table, "tidx", "hit")
			}

			ch <- c.sizes.newConstMetric(stat.sizebytes, stat.database, stat.schema, stat.table)
			ch <- c.reltuples.newConstMetric(stat.reltuples, stat.database, stat.schema, stat.table)
		}
	}

	return nil
}

// postgresTableStat is per-table store for metrics related to how tables are accessed.
type postgresTableStat struct {
	database        string
	schema          string
	table           string
	seqscan         float64
	seqtupread      float64
	idxscan         float64
	idxtupfetch     float64
	inserted        float64
	updated         float64
	deleted         float64
	hotUpdated      float64
	live            float64
	dead            float64
	modified        float64
	lastvacuumAge   float64
	lastanalyzeAge  float64
	lastvacuumTime  float64
	lastanalyzeTime float64
	vacuum          float64
	autovacuum      float64
	analyze         float64
	autoanalyze     float64
	heapread        float64
	heaphit         float64
	idxread         float64
	idxhit          float64
	toastread       float64
	toasthit        float64
	tidxread        float64
	tidxhit         float64
	sizebytes       float64
	reltuples       float64
}

// parsePostgresTableStats parses PGResult and returns structs with stats values.
func parsePostgresTableStats(r *model.PGResult, labelNames []string) map[string]postgresTableStat {
	log.Debug("parse postgres tables stats")

	var stats = make(map[string]postgresTableStat)

	var tablename string

	for _, row := range r.Rows {
		table := postgresTableStat{}
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "database":
				table.database = row[i].String
			case "schema":
				table.schema = row[i].String
			case "table":
				table.table = row[i].String
			}
		}

		// create a table name consisting of trio database/schema/table
		tablename = strings.Join([]string{table.database, table.schema, table.table}, "/")

		stats[tablename] = table

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

			s := stats[tablename]

			switch string(colname.Name) {
			case "seq_scan":
				s.seqscan = v
			case "seq_tup_read":
				s.seqtupread = v
			case "idx_scan":
				s.idxscan = v
			case "idx_tup_fetch":
				s.idxtupfetch = v
			case "n_tup_ins":
				s.inserted = v
			case "n_tup_upd":
				s.updated = v
			case "n_tup_del":
				s.deleted = v
			case "n_tup_hot_upd":
				s.hotUpdated = v
			case "n_live_tup":
				s.live = v
			case "n_dead_tup":
				s.dead = v
			case "n_mod_since_analyze":
				s.modified = v
			case "last_vacuum_seconds":
				s.lastvacuumAge = v
			case "last_analyze_seconds":
				s.lastanalyzeAge = v
			case "last_vacuum_time":
				s.lastvacuumTime = v
			case "last_analyze_time":
				s.lastanalyzeTime = v
			case "vacuum_count":
				s.vacuum = v
			case "autovacuum_count":
				s.autovacuum = v
			case "analyze_count":
				s.analyze = v
			case "autoanalyze_count":
				s.autoanalyze = v
			case "heap_blks_read":
				s.heapread = v
			case "heap_blks_hit":
				s.heaphit = v
			case "idx_blks_read":
				s.idxread = v
			case "idx_blks_hit":
				s.idxhit = v
			case "toast_blks_read":
				s.toastread = v
			case "toast_blks_hit":
				s.toasthit = v
			case "tidx_blks_read":
				s.tidxread = v
			case "tidx_blks_hit":
				s.tidxhit = v
			case "size_bytes":
				s.sizebytes = v
			case "reltuples":
				s.reltuples = v
			default:
				continue
			}

			stats[tablename] = s
		}
	}

	return stats
}
