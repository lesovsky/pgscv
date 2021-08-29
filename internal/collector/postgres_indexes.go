package collector

import (
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"strconv"
	"strings"
)

const (
	userIndexesQuery = "SELECT current_database() AS database, schemaname AS schema, relname AS table, indexrelname AS index, (i.indisprimary OR i.indisunique) AS key," +
		"idx_scan, idx_tup_read, idx_tup_fetch, idx_blks_read, idx_blks_hit,pg_relation_size(s1.indexrelid) AS size_bytes " +
		"FROM pg_stat_user_indexes s1 " +
		"JOIN pg_statio_user_indexes s2 USING (schemaname, relname, indexrelname) " +
		"JOIN pg_index i ON (s1.indexrelid = i.indexrelid) " +
		"WHERE NOT EXISTS (SELECT 1 FROM pg_locks WHERE relation = s1.indexrelid AND mode = 'AccessExclusiveLock' AND granted)"
)

// postgresIndexesCollector defines metric descriptors and stats store.
type postgresIndexesCollector struct {
	indexes typedDesc
	tuples  typedDesc
	io      typedDesc
	sizes   typedDesc
}

// NewPostgresIndexesCollector returns a new Collector exposing postgres indexes stats.
// For details see
// https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ALL-INDEXES-VIEW
// https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STATIO-ALL-INDEXES-VIEW
func NewPostgresIndexesCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &postgresIndexesCollector{
		indexes: newBuiltinTypedDesc(
			descOpts{"postgres", "index", "scans_total", "Total number of index scans initiated.", 0},
			prometheus.CounterValue,
			[]string{"database", "schema", "table", "index", "key"}, constLabels,
			settings.Filters,
		),
		tuples: newBuiltinTypedDesc(
			descOpts{"postgres", "index", "tuples_total", "Total number of index entries processed by scans.", 0},
			prometheus.CounterValue,
			[]string{"database", "schema", "table", "index", "tuples"}, constLabels,
			settings.Filters,
		),
		io: newBuiltinTypedDesc(
			descOpts{"postgres", "index_io", "blocks_total", "Total number of indexes' blocks processed.", 0},
			prometheus.CounterValue,
			[]string{"database", "schema", "table", "index", "access"}, constLabels,
			settings.Filters,
		),
		sizes: newBuiltinTypedDesc(
			descOpts{"postgres", "index", "size_bytes", "Total size of the index, in bytes.", 0},
			prometheus.GaugeValue,
			[]string{"database", "schema", "table", "index"}, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresIndexesCollector) Update(config Config, ch chan<- prometheus.Metric) error {
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

		res, err := conn.Query(userIndexesQuery)
		conn.Close()
		if err != nil {
			log.Warnf("get indexes stat of database %s failed: %s", d, err)
			continue
		}

		stats := parsePostgresIndexStats(res, c.indexes.labelNames)

		for _, stat := range stats {
			// always send idx scan metrics and indexes size
			ch <- c.indexes.newConstMetric(stat.idxscan, stat.database, stat.schema, stat.table, stat.index, stat.key)
			ch <- c.sizes.newConstMetric(stat.sizebytes, stat.database, stat.schema, stat.table, stat.index)

			// avoid metrics spamming and send metrics only if they greater than zero.
			if stat.idxtupread > 0 {
				ch <- c.tuples.newConstMetric(stat.idxread, stat.database, stat.schema, stat.table, stat.index, "read")
			}
			if stat.idxtupfetch > 0 {
				ch <- c.tuples.newConstMetric(stat.idxtupfetch, stat.database, stat.schema, stat.table, stat.index, "fetched")
			}
			if stat.idxread > 0 {
				ch <- c.io.newConstMetric(stat.idxread, stat.database, stat.schema, stat.table, stat.index, "read")
			}
			if stat.idxhit > 0 {
				ch <- c.io.newConstMetric(stat.idxhit, stat.database, stat.schema, stat.table, stat.index, "hit")
			}
		}
	}

	return nil
}

// postgresIndexStat is per-index store for metrics related to how indexes are accessed.
type postgresIndexStat struct {
	database    string
	schema      string
	table       string
	index       string
	key         string
	idxscan     float64
	idxtupread  float64
	idxtupfetch float64
	idxread     float64
	idxhit      float64
	sizebytes   float64
}

// parsePostgresIndexStats parses PGResult and returns structs with stats values.
func parsePostgresIndexStats(r *model.PGResult, labelNames []string) map[string]postgresIndexStat {
	log.Debug("parse postgres indexes stats")

	var stats = make(map[string]postgresIndexStat)

	var indexname string

	for _, row := range r.Rows {
		index := postgresIndexStat{}
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "database":
				index.database = row[i].String
			case "schema":
				index.schema = row[i].String
			case "table":
				index.table = row[i].String
			case "index":
				index.index = row[i].String
			case "key":
				index.key = row[i].String
			}
		}

		// create a index name consisting of quartet database/schema/table/index
		indexname = strings.Join([]string{index.database, index.schema, index.table, index.index}, "/")

		stats[indexname] = index

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

			s := stats[indexname]

			switch string(colname.Name) {
			case "idx_scan":
				s.idxscan = v
			case "idx_tup_read":
				s.idxtupread = v
			case "idx_tup_fetch":
				s.idxtupfetch = v
			case "idx_blks_read":
				s.idxread = v
			case "idx_blks_hit":
				s.idxhit = v
			case "size_bytes":
				s.sizebytes = v
			default:
				continue
			}

			stats[indexname] = s
		}
	}

	return stats
}
