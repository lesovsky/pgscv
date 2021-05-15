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
	userIndexesQuery = "SELECT current_database() AS datname, schemaname, relname, indexrelname, (i.indisprimary OR i.indisunique) AS key," +
		"idx_scan, idx_tup_read, idx_tup_fetch, idx_blks_read, idx_blks_hit,pg_relation_size(s1.indexrelid) AS size_bytes " +
		"FROM pg_stat_user_indexes s1 " +
		"JOIN pg_statio_user_indexes s2 USING (schemaname, relname, indexrelname) " +
		"JOIN pg_index i ON (s1.indexrelid = i.indexrelid) " +
		"WHERE NOT EXISTS (SELECT 1 FROM pg_locks WHERE relation = s1.indexrelid AND mode = 'AccessExclusiveLock' AND granted)"
)

// postgresIndexesCollector defines metric descriptors and stats store.
type postgresIndexesCollector struct {
	indexes    typedDesc
	tuples     typedDesc
	io         typedDesc
	sizes      typedDesc
	labelNames []string
}

// NewPostgresIndexesCollector returns a new Collector exposing postgres indexes stats.
// For details see
// https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ALL-INDEXES-VIEW
// https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STATIO-ALL-INDEXES-VIEW
func NewPostgresIndexesCollector(constLabels prometheus.Labels, _ model.CollectorSettings) (Collector, error) {
	var tablesLabelNames = []string{"datname", "schemaname", "relname", "indexrelname", "key"}

	return &postgresIndexesCollector{
		labelNames: tablesLabelNames,
		indexes: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "index", "scans_total"),
				"Total number of index scans initiated.",
				tablesLabelNames, constLabels,
			),
			valueType: prometheus.CounterValue,
		},
		tuples: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "index", "tuples_total"),
				"Total number of index entries processed by scans.",
				[]string{"datname", "schemaname", "relname", "indexrelname", "op"}, constLabels,
			),
			valueType: prometheus.CounterValue,
		},
		io: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "index_io", "blocks_total"),
				"Total number of indexes' blocks processed.",
				[]string{"datname", "schemaname", "relname", "indexrelname", "cache_hit"}, constLabels,
			),
			valueType: prometheus.CounterValue,
		},
		sizes: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "index", "size_bytes"),
				"Total size of the index, in bytes.",
				[]string{"datname", "schemaname", "relname", "indexrelname"}, constLabels,
			),
			valueType: prometheus.GaugeValue,
		},
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

		stats := parsePostgresIndexStats(res, c.labelNames)

		for _, stat := range stats {
			// always send idx scan metrics and indexes size
			ch <- c.indexes.mustNewConstMetric(stat.idxscan, stat.datname, stat.schemaname, stat.relname, stat.indexname, stat.key)
			ch <- c.sizes.mustNewConstMetric(stat.sizebytes, stat.datname, stat.schemaname, stat.relname, stat.indexname)

			// avoid metrics spamming and send metrics only if they greater than zero.
			if stat.idxtupread > 0 {
				ch <- c.tuples.mustNewConstMetric(stat.idxread, stat.datname, stat.schemaname, stat.relname, stat.indexname, "read")
			}
			if stat.idxtupfetch > 0 {
				ch <- c.tuples.mustNewConstMetric(stat.idxtupfetch, stat.datname, stat.schemaname, stat.relname, stat.indexname, "fetch")
			}
			if stat.idxread > 0 {
				ch <- c.io.mustNewConstMetric(stat.idxread, stat.datname, stat.schemaname, stat.relname, stat.indexname, "false")
			}
			if stat.idxhit > 0 {
				ch <- c.io.mustNewConstMetric(stat.idxhit, stat.datname, stat.schemaname, stat.relname, stat.indexname, "true")
			}
		}
	}

	return nil
}

// postgresIndexStat is per-index store for metrics related to how indexes are accessed.
type postgresIndexStat struct {
	datname     string
	schemaname  string
	relname     string
	indexname   string
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
			case "datname":
				index.datname = row[i].String
			case "schemaname":
				index.schemaname = row[i].String
			case "relname":
				index.relname = row[i].String
			case "indexrelname":
				index.indexname = row[i].String
			case "key":
				index.key = row[i].String
			}
		}

		// create a index name consisting of quartet database/schema/table/index
		indexname = strings.Join([]string{index.datname, index.schemaname, index.relname, index.indexname}, "/")

		stats[indexname] = index

		for i, colname := range r.Colnames {
			// skip columns if its value used as a label
			if stringsContains(labelNames, string(colname.Name)) {
				log.Debugf("skip label mapped column '%s'", string(colname.Name))
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

			switch string(colname.Name) {
			case "idx_scan":
				s := stats[indexname]
				s.idxscan = v
				stats[indexname] = s
			case "idx_tup_read":
				s := stats[indexname]
				s.idxtupread = v
				stats[indexname] = s
			case "idx_tup_fetch":
				s := stats[indexname]
				s.idxtupfetch = v
				stats[indexname] = s
			case "idx_blks_read":
				s := stats[indexname]
				s.idxread = v
				stats[indexname] = s
			case "idx_blks_hit":
				s := stats[indexname]
				s.idxhit = v
				stats[indexname] = s
			case "size_bytes":
				s := stats[indexname]
				s.sizebytes = v
				stats[indexname] = s
			default:
				log.Debugf("unsupported pg_stat_user_indexes (or pg_statio_user_indexes) stat column: %s, skip", string(colname.Name))
				continue
			}
		}
	}

	return stats
}
