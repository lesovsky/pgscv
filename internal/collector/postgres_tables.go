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

const (
	userTablesQuery = `SELECT
    current_database() AS datname, s1.schemaname, s1.relname,
    seq_scan, seq_tup_read,
    idx_scan, idx_tup_fetch,
    n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd,
    n_live_tup, n_dead_tup, n_mod_since_analyze,
    extract('epoch' from age(now(), greatest(last_vacuum, last_autovacuum))) as last_vacuum_seconds,
    extract('epoch' from age(now(), greatest(last_analyze, last_autoanalyze))) as last_analyze_seconds,
    vacuum_count, autovacuum_count, analyze_count, autoanalyze_count,
    heap_blks_read, heap_blks_hit, idx_blks_read, idx_blks_hit, toast_blks_read, toast_blks_hit, tidx_blks_read, tidx_blks_hit,
    pg_relation_size(s1.relid) AS size_bytes
FROM pg_stat_user_tables s1
JOIN pg_statio_user_tables s2 USING (schemaname, relname)
WHERE NOT EXISTS (SELECT 1 FROM pg_locks WHERE relation = s1.relid AND mode = 'AccessExclusiveLock' AND granted)`
)

// postgresTablesCollector defines metric descriptors and stats store.
type postgresTablesCollector struct {
	seqscan          typedDesc
	seqtupread       typedDesc
	idxscan          typedDesc
	idxtupfetch      typedDesc
	tuples           typedDesc
	tuplestotal      typedDesc
	maintLastVacuum  typedDesc
	maintLastAnalyze typedDesc
	maintenance      typedDesc
	io               typedDesc
	sizes            typedDesc
	labelNames       []string
}

// NewPostgresTablesCollector returns a new Collector exposing postgres tables stats.
// For details see
// https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ALL-TABLES-VIEW
// https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STATIO-ALL-TABLES-VIEW
func NewPostgresTablesCollector(constLabels prometheus.Labels) (Collector, error) {
	var tablesLabelNames = []string{"datname", "schemaname", "relname"}

	return &postgresTablesCollector{
		labelNames: tablesLabelNames,
		seqscan: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "table", "seq_scan_total"),
				"The total number of sequential scans have been done.",
				tablesLabelNames, constLabels,
			),
			valueType: prometheus.CounterValue,
		},
		seqtupread: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "table", "seq_tup_read_total"),
				"The total number of tuples have been read by sequential scans.",
				tablesLabelNames, constLabels,
			),
			valueType: prometheus.CounterValue,
		},
		idxscan: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "table", "idx_scan_total"),
				"Total number of index scans initiated on this table.",
				tablesLabelNames, constLabels,
			),
			valueType: prometheus.CounterValue,
		},
		idxtupfetch: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "table", "idx_tup_fetch_total"),
				"Total number of live rows fetched by index scans.",
				tablesLabelNames, constLabels,
			),
			valueType: prometheus.CounterValue,
		},
		tuples: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "table", "tuples_modified_total"),
				"Total number of operations have been made on rows in the table.",
				[]string{"datname", "schemaname", "relname", "operation"}, constLabels,
			),
			valueType: prometheus.CounterValue,
		},
		tuplestotal: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "table", "tuples_total"),
				"Estimated total number of rows in the table.",
				[]string{"datname", "schemaname", "relname", "type"}, constLabels,
			),
			valueType: prometheus.GaugeValue,
		},
		maintLastVacuum: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "table", "last_vacuum_seconds"),
				"Time since table was vacuumed manually or automatically (not counting VACUUM FULL), in seconds.",
				tablesLabelNames, constLabels,
			),
			valueType: prometheus.CounterValue,
		},
		maintLastAnalyze: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "table", "last_analyze_seconds"),
				"Time since table was analyzed manually or automatically, in seconds.",
				tablesLabelNames, constLabels,
			),
			valueType: prometheus.CounterValue,
		},
		maintenance: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "table", "maintenance_total"),
				"Total number of times this table has been vacuumed or analyzed.",
				[]string{"datname", "schemaname", "relname", "type"}, constLabels,
			),
			valueType: prometheus.CounterValue,
		},
		io: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "table_io", "blocks_total"),
				"Total number of table's blocks processed.",
				[]string{"datname", "schemaname", "relname", "type", "cache_hit"}, constLabels,
			),
			valueType: prometheus.CounterValue,
		},
		sizes: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "table", "size_bytes_total"),
				"Total size of the table, in bytes.",
				[]string{"datname", "schemaname", "relname"}, constLabels,
			),
			valueType: prometheus.CounterValue,
		},
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
		pgconfig.Database = d
		conn, err := store.NewWithConfig(pgconfig)
		if err != nil {
			return err
		}

		res, err := conn.Query(userTablesQuery)
		conn.Close()
		if err != nil {
			log.Warnf("get tables stat of database %s failed: %s", d, err)
			continue
		}

		stats := parsePostgresTableStats(res, c.labelNames)

		for _, stat := range stats {
			// scan stats
			ch <- c.seqscan.mustNewConstMetric(stat.seqscan, stat.datname, stat.schemaname, stat.relname)
			ch <- c.seqtupread.mustNewConstMetric(stat.seqtupread, stat.datname, stat.schemaname, stat.relname)
			ch <- c.idxscan.mustNewConstMetric(stat.idxscan, stat.datname, stat.schemaname, stat.relname)
			ch <- c.idxtupfetch.mustNewConstMetric(stat.idxtupfetch, stat.datname, stat.schemaname, stat.relname)

			// tuples stats
			ch <- c.tuples.mustNewConstMetric(stat.inserted, stat.datname, stat.schemaname, stat.relname, "inserted")
			ch <- c.tuples.mustNewConstMetric(stat.updated, stat.datname, stat.schemaname, stat.relname, "updated")
			ch <- c.tuples.mustNewConstMetric(stat.deleted, stat.datname, stat.schemaname, stat.relname, "deleted")
			ch <- c.tuples.mustNewConstMetric(stat.hotUpdated, stat.datname, stat.schemaname, stat.relname, "hot_updated")

			// tuples total stats
			ch <- c.tuplestotal.mustNewConstMetric(stat.live, stat.datname, stat.schemaname, stat.relname, "live")
			ch <- c.tuplestotal.mustNewConstMetric(stat.dead, stat.datname, stat.schemaname, stat.relname, "dead")
			ch <- c.tuplestotal.mustNewConstMetric(stat.modified, stat.datname, stat.schemaname, stat.relname, "modified")

			// maintenance stats -- avoid metrics spam produced by inactive tables, don't send metrics if counters are zero.
			if stat.lastvacuum > 0 {
				ch <- c.maintLastVacuum.mustNewConstMetric(stat.lastvacuum, stat.datname, stat.schemaname, stat.relname)
			}
			if stat.lastanalyze > 0 {
				ch <- c.maintLastAnalyze.mustNewConstMetric(stat.lastanalyze, stat.datname, stat.schemaname, stat.relname)
			}
			if stat.vacuum > 0 {
				ch <- c.maintenance.mustNewConstMetric(stat.vacuum, stat.datname, stat.schemaname, stat.relname, "vacuum")
			}
			if stat.autovacuum > 0 {
				ch <- c.maintenance.mustNewConstMetric(stat.autovacuum, stat.datname, stat.schemaname, stat.relname, "autovacuum")
			}
			if stat.analyze > 0 {
				ch <- c.maintenance.mustNewConstMetric(stat.analyze, stat.datname, stat.schemaname, stat.relname, "analyze")
			}
			if stat.autoanalyze > 0 {
				ch <- c.maintenance.mustNewConstMetric(stat.autoanalyze, stat.datname, stat.schemaname, stat.relname, "autoanalyze")
			}

			// io stats -- avoid metrics spam produced by inactive tables, don't send metrics if counters are zero.
			if stat.heapread > 0 {
				ch <- c.io.mustNewConstMetric(stat.heapread, stat.datname, stat.schemaname, stat.relname, "heap", "false")
			}
			if stat.heaphit > 0 {
				ch <- c.io.mustNewConstMetric(stat.heaphit, stat.datname, stat.schemaname, stat.relname, "heap", "true")
			}
			if stat.idxread > 0 {
				ch <- c.io.mustNewConstMetric(stat.idxread, stat.datname, stat.schemaname, stat.relname, "idx", "false")
			}
			if stat.idxhit > 0 {
				ch <- c.io.mustNewConstMetric(stat.idxhit, stat.datname, stat.schemaname, stat.relname, "idx", "true")
			}
			if stat.toastread > 0 {
				ch <- c.io.mustNewConstMetric(stat.toastread, stat.datname, stat.schemaname, stat.relname, "toast", "false")
			}
			if stat.toasthit > 0 {
				ch <- c.io.mustNewConstMetric(stat.toasthit, stat.datname, stat.schemaname, stat.relname, "toast", "true")
			}
			if stat.tidxread > 0 {
				ch <- c.io.mustNewConstMetric(stat.tidxread, stat.datname, stat.schemaname, stat.relname, "tidx", "false")
			}
			if stat.tidxhit > 0 {
				ch <- c.io.mustNewConstMetric(stat.tidxhit, stat.datname, stat.schemaname, stat.relname, "tidx", "true")
			}

			ch <- c.sizes.mustNewConstMetric(stat.sizebytes, stat.datname, stat.schemaname, stat.relname)
		}
	}

	return nil
}

// postgresTableStat is per-table store for metrics related to how tables are accessed.
type postgresTableStat struct {
	datname     string
	schemaname  string
	relname     string
	seqscan     float64
	seqtupread  float64
	idxscan     float64
	idxtupfetch float64
	inserted    float64
	updated     float64
	deleted     float64
	hotUpdated  float64
	live        float64
	dead        float64
	modified    float64
	lastvacuum  float64
	lastanalyze float64
	vacuum      float64
	autovacuum  float64
	analyze     float64
	autoanalyze float64
	heapread    float64
	heaphit     float64
	idxread     float64
	idxhit      float64
	toastread   float64
	toasthit    float64
	tidxread    float64
	tidxhit     float64
	sizebytes   float64
}

// parsePostgresTableStats parses PGResult and returns structs with stats values.
func parsePostgresTableStats(r *model.PGResult, labelNames []string) map[string]postgresTableStat {
	var stats = make(map[string]postgresTableStat)

	var tablename string

	for _, row := range r.Rows {
		table := postgresTableStat{}
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "datname":
				table.datname = row[i].String
			case "schemaname":
				table.schemaname = row[i].String
			case "relname":
				table.relname = row[i].String
			}
		}

		// create a table name consisting of trio database/schema/table
		tablename = strings.Join([]string{table.datname, table.schemaname, table.relname}, "/")

		stats[tablename] = table

		for i, colname := range r.Colnames {
			// Column's values act as metric values or as labels values.
			// If column's name is NOT in the labelNames, process column's values as values for metrics. If column's name
			// is in the labelNames, skip that column.
			if !stringsContains(labelNames, string(colname.Name)) {
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

				switch string(colname.Name) {
				case "seq_scan":
					s := stats[tablename]
					s.seqscan = v
					stats[tablename] = s
				case "seq_tup_read":
					s := stats[tablename]
					s.seqtupread = v
					stats[tablename] = s
				case "idx_scan":
					s := stats[tablename]
					s.idxscan = v
					stats[tablename] = s
				case "idx_tup_fetch":
					s := stats[tablename]
					s.idxtupfetch = v
					stats[tablename] = s
				case "n_tup_ins":
					s := stats[tablename]
					s.inserted = v
					stats[tablename] = s
				case "n_tup_upd":
					s := stats[tablename]
					s.updated = v
					stats[tablename] = s
				case "n_tup_del":
					s := stats[tablename]
					s.deleted = v
					stats[tablename] = s
				case "n_tup_hot_upd":
					s := stats[tablename]
					s.hotUpdated = v
					stats[tablename] = s
				case "n_live_tup":
					s := stats[tablename]
					s.live = v
					stats[tablename] = s
				case "n_dead_tup":
					s := stats[tablename]
					s.dead = v
					stats[tablename] = s
				case "n_mod_since_analyze":
					s := stats[tablename]
					s.modified = v
					stats[tablename] = s
				case "last_vacuum_seconds":
					s := stats[tablename]
					s.lastvacuum = v
					stats[tablename] = s
				case "last_analyze_seconds":
					s := stats[tablename]
					s.lastanalyze = v
					stats[tablename] = s
				case "vacuum_count":
					s := stats[tablename]
					s.vacuum = v
					stats[tablename] = s
				case "autovacuum_count":
					s := stats[tablename]
					s.autovacuum = v
					stats[tablename] = s
				case "analyze_count":
					s := stats[tablename]
					s.analyze = v
					stats[tablename] = s
				case "autoanalyze_count":
					s := stats[tablename]
					s.autoanalyze = v
					stats[tablename] = s
				case "heap_blks_read":
					s := stats[tablename]
					s.heapread = v
					stats[tablename] = s
				case "heap_blks_hit":
					s := stats[tablename]
					s.heaphit = v
					stats[tablename] = s
				case "idx_blks_read":
					s := stats[tablename]
					s.idxread = v
					stats[tablename] = s
				case "idx_blks_hit":
					s := stats[tablename]
					s.idxhit = v
					stats[tablename] = s
				case "toast_blks_read":
					s := stats[tablename]
					s.toastread = v
					stats[tablename] = s
				case "toast_blks_hit":
					s := stats[tablename]
					s.toasthit = v
					stats[tablename] = s
				case "tidx_blks_read":
					s := stats[tablename]
					s.tidxread = v
					stats[tablename] = s
				case "tidx_blks_hit":
					s := stats[tablename]
					s.tidxhit = v
					stats[tablename] = s
				case "size_bytes":
					s := stats[tablename]
					s.sizebytes = v
					stats[tablename] = s
				default:
					log.Debugf("unsupported pg_stat_user_tables (or pg_statio_user_tables) stat column: %s, skip", string(colname.Name))
					continue
				}
			}
		}
	}

	return stats
}
