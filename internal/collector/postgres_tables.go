package collector

import (
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"strings"
)

const userTablesQuery = `SELECT
  current_database() AS datname, schemaname, relname,
  seq_scan, seq_tup_read,
  idx_scan, idx_tup_fetch,
  n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd,
  n_live_tup, n_dead_tup, n_mod_since_analyze,
  coalesce(extract('epoch' from age(now(), greatest(last_vacuum, last_autovacuum))), 0) as last_vacuum_seconds,
  coalesce(extract('epoch' from age(now(), greatest(last_analyze, last_autoanalyze))), 0) as last_analyze_seconds,
  vacuum_count, autovacuum_count, analyze_count, autoanalyze_count
FROM pg_stat_user_tables`

// scanStat is per-table store for metrics related to how tables are accessed.
type scanStat struct {
	seqscan     float64
	seqtupread  float64
	idxscan     float64
	idxtupfetch float64
}

// tuplesStat is a per-table store for metrics related to how many tuples were modified.
type tuplesStat struct {
	inserted   float64
	updated    float64
	deleted    float64
	hotUpdated float64
}

// tuplesTotalStat is a per-table store for metrics related how many tuples in the table.
type tuplesTotalStat struct {
	live     float64
	dead     float64
	modified float64
}

// maintenanceStat is a per-table store for metrics related to maintenance operations like vacuum or analyze.
type maintenanceStat struct {
	lastvacuum  float64
	vacuum      float64
	autovacuum  float64
	lastanalyze float64
	analyze     float64
	autoanalyze float64
}

// tablesStats is cumulative store for all tables stats in processed database.
type tablesStats struct {
	scanStats        map[string]scanStat
	tupleStats       map[string]tuplesStat
	tupleTotalStats  map[string]tuplesTotalStat
	maintenanceStats map[string]maintenanceStat
}

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
	labelNames       []string
}

// NewPostgresTablesCollector returns a new Collector exposing postgres tables stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ALL-TABLES-VIEW
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
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresTablesCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.NewDB(config.ConnString)
	if err != nil {
		return err
	}

	databases, err := conn.GetDatabases()
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
		conn, err := store.NewDBConfig(pgconfig)
		if err != nil {
			return err
		}

		res, err := conn.GetStats(userTablesQuery)
		if err != nil {
			return err
		}

		stats := parsePostgresTableStats(res, c.labelNames)

		for tablename, stat := range stats.scanStats {
			props := strings.Split(tablename, "/")
			if len(props) != 3 {
				log.Warnf("incomplete pool name: %s; skip", tablename)
				continue
			}
			datname, schema, relname := props[0], props[1], props[2]
			ch <- c.seqscan.mustNewConstMetric(stat.seqscan, datname, schema, relname)
			ch <- c.seqtupread.mustNewConstMetric(stat.seqtupread, datname, schema, relname)
			ch <- c.idxscan.mustNewConstMetric(stat.idxscan, datname, schema, relname)
			ch <- c.idxtupfetch.mustNewConstMetric(stat.idxtupfetch, datname, schema, relname)
		}

		for tablename, stat := range stats.tupleStats {
			props := strings.Split(tablename, "/")
			if len(props) != 3 {
				log.Warnf("incomplete pool name: %s; skip", tablename)
				continue
			}
			datname, schema, relname := props[0], props[1], props[2]
			ch <- c.tuples.mustNewConstMetric(stat.inserted, datname, schema, relname, "inserted")
			ch <- c.tuples.mustNewConstMetric(stat.updated, datname, schema, relname, "updated")
			ch <- c.tuples.mustNewConstMetric(stat.deleted, datname, schema, relname, "deleted")
			ch <- c.tuples.mustNewConstMetric(stat.hotUpdated, datname, schema, relname, "hot_updated")
		}

		for tablename, stat := range stats.tupleTotalStats {
			props := strings.Split(tablename, "/")
			if len(props) != 3 {
				log.Warnf("incomplete pool name: %s; skip", tablename)
				continue
			}
			datname, schema, relname := props[0], props[1], props[2]
			ch <- c.tuplestotal.mustNewConstMetric(stat.live, datname, schema, relname, "live")
			ch <- c.tuplestotal.mustNewConstMetric(stat.dead, datname, schema, relname, "dead")
			ch <- c.tuplestotal.mustNewConstMetric(stat.modified, datname, schema, relname, "modified")
		}

		for tablename, stat := range stats.maintenanceStats {
			props := strings.Split(tablename, "/")
			if len(props) != 3 {
				log.Warnf("incomplete pool name: %s; skip", tablename)
				continue
			}
			datname, schema, relname := props[0], props[1], props[2]
			ch <- c.maintLastVacuum.mustNewConstMetric(stat.lastvacuum, datname, schema, relname)
			ch <- c.maintLastAnalyze.mustNewConstMetric(stat.lastanalyze, datname, schema, relname)
			ch <- c.maintenance.mustNewConstMetric(stat.vacuum, datname, schema, relname, "vacuum")
			ch <- c.maintenance.mustNewConstMetric(stat.autovacuum, datname, schema, relname, "autovacuum")
			ch <- c.maintenance.mustNewConstMetric(stat.analyze, datname, schema, relname, "analyze")
			ch <- c.maintenance.mustNewConstMetric(stat.autoanalyze, datname, schema, relname, "autoanalyze")
		}
	}

	return nil
}

func parsePostgresTableStats(r *store.QueryResult, labelNames []string) tablesStats {
	// ad-hoc struct used to group pool properties (database, user and mode) in one place.
	type tableFQName struct {
		datname    string
		schemaname string
		relname    string
	}

	var stats = tablesStats{
		scanStats:        map[string]scanStat{},
		tupleStats:       map[string]tuplesStat{},
		tupleTotalStats:  map[string]tuplesTotalStat{},
		maintenanceStats: map[string]maintenanceStat{},
	}
	var tablename string

	for _, row := range r.Rows {
		table := tableFQName{}
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

		// create a pool name consisting of trio database/user/pool_mode
		tablename = strings.Join([]string{table.datname, table.schemaname, table.relname}, "/")

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
					s := stats.scanStats[tablename]
					s.seqscan = v
					stats.scanStats[tablename] = s
				case "seq_tup_read":
					s := stats.scanStats[tablename]
					s.seqtupread = v
					stats.scanStats[tablename] = s
				case "idx_scan":
					s := stats.scanStats[tablename]
					s.idxscan = v
					stats.scanStats[tablename] = s
				case "idx_tup_fetch":
					s := stats.scanStats[tablename]
					s.idxtupfetch = v
					stats.scanStats[tablename] = s
				case "n_tup_ins":
					s := stats.tupleStats[tablename]
					s.inserted = v
					stats.tupleStats[tablename] = s
				case "n_tup_upd":
					s := stats.tupleStats[tablename]
					s.updated = v
					stats.tupleStats[tablename] = s
				case "n_tup_del":
					s := stats.tupleStats[tablename]
					s.deleted = v
					stats.tupleStats[tablename] = s
				case "n_tup_hot_upd":
					s := stats.tupleStats[tablename]
					s.hotUpdated = v
					stats.tupleStats[tablename] = s
				case "n_live_tup":
					s := stats.tupleTotalStats[tablename]
					s.live = v
					stats.tupleTotalStats[tablename] = s
				case "n_dead_tup":
					s := stats.tupleTotalStats[tablename]
					s.dead = v
					stats.tupleTotalStats[tablename] = s
				case "n_mod_since_analyze":
					s := stats.tupleTotalStats[tablename]
					s.modified = v
					stats.tupleTotalStats[tablename] = s
				case "last_vacuum_seconds":
					s := stats.maintenanceStats[tablename]
					s.lastvacuum = v
					stats.maintenanceStats[tablename] = s
				case "last_analyze_seconds":
					s := stats.maintenanceStats[tablename]
					s.lastanalyze = v
					stats.maintenanceStats[tablename] = s
				case "vacuum_count":
					s := stats.maintenanceStats[tablename]
					s.vacuum = v
					stats.maintenanceStats[tablename] = s
				case "autovacuum_count":
					s := stats.maintenanceStats[tablename]
					s.autovacuum = v
					stats.maintenanceStats[tablename] = s
				case "analyze_count":
					s := stats.maintenanceStats[tablename]
					s.analyze = v
					stats.maintenanceStats[tablename] = s
				case "autoanalyze_count":
					s := stats.maintenanceStats[tablename]
					s.autoanalyze = v
					stats.maintenanceStats[tablename] = s
				}
			}
		}
	}

	return stats
}
