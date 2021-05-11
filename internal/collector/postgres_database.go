package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"strconv"
)

const (
	databaseQuery = "SELECT " +
		"COALESCE(datname, '__shared__') AS datname, " +
		"xact_commit, xact_rollback, blks_read, blks_hit, tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted, " +
		"conflicts, temp_files, temp_bytes, deadlocks, blk_read_time, blk_write_time, pg_database_size(datname) as size_bytes, " +
		"coalesce(extract('epoch' from age(now(), stats_reset)), 0) as stats_age_seconds " +
		"FROM pg_stat_database WHERE datname IN (SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate)"

	xidLimitQuery = "SELECT 'database' AS src, 2147483647 - greatest(max(age(datfrozenxid)), max(age(coalesce(nullif(datminmxid, 1), datfrozenxid)))) AS to_limit FROM pg_database " +
		"UNION SELECT 'prepared_xacts' AS src, 2147483647 - coalesce(max(age(transaction)), 0) AS to_limit FROM pg_prepared_xacts " +
		"UNION SELECT 'replication_slots' AS src, 2147483647 - greatest(coalesce(min(age(xmin)), 0), coalesce(min(age(catalog_xmin)), 0)) AS to_limit FROM pg_replication_slots"
)

type postgresDatabasesCollector struct {
	commits    typedDesc
	rollbacks  typedDesc
	conflicts  typedDesc
	deadlocks  typedDesc
	blocks     typedDesc
	tuples     typedDesc
	tempbytes  typedDesc
	tempfiles  typedDesc
	blockstime typedDesc
	sizes      typedDesc
	statsage   typedDesc
	xidlimit   typedDesc
	labelNames []string
	custom     []typedDescSet
}

// NewPostgresDatabasesCollector returns a new Collector exposing postgres databases stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-DATABASE-VIEW
func NewPostgresDatabasesCollector(constLabels prometheus.Labels, settings model.CollectorSettings) (Collector, error) {
	// This instance of builtinSubsystems just used for detecting collisions with user-defined metrics.
	// This must be always synchronized with metric descriptors in returned value.
	builtinSubsystems := model.Subsystems{
		"database": {
			Metrics: model.Metrics{
				{ShortName: "datname"},
				{ShortName: "xact_commits_total"},
				{ShortName: "xact_rollbacks_total"},
				{ShortName: "conflicts_total"},
				{ShortName: "deadlocks_total"},
				{ShortName: "blocks_total"},
				{ShortName: "tuples_total"},
				{ShortName: "temp_bytes_total"},
				{ShortName: "temp_files_total"},
				{ShortName: "blk_time_seconds"},
				{ShortName: "size_bytes"},
				{ShortName: "stats_age_seconds"},
			},
		},
		"xacts": {
			Metrics: model.Metrics{
				{ShortName: "xid"},
				{ShortName: "left_before_wraparound"},
			},
		},
	}

	removeCollisions(builtinSubsystems, settings.Subsystems)

	databaseLabelNames := []string{"datname"}

	return &postgresDatabasesCollector{
		labelNames: databaseLabelNames,
		commits: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "database", "xact_commits_total"),
				"Total number of transactions had been committed.",
				databaseLabelNames, constLabels,
			), valueType: prometheus.CounterValue,
		},
		rollbacks: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "database", "xact_rollbacks_total"),
				"Total number of transactions had been rolled back.",
				databaseLabelNames, constLabels,
			), valueType: prometheus.CounterValue,
		},
		conflicts: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "database", "conflicts_total"),
				"Total number of recovery conflicts occurred.",
				databaseLabelNames, constLabels,
			), valueType: prometheus.CounterValue,
		},
		deadlocks: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "database", "deadlocks_total"),
				"Total number of deadlocks occurred.",
				databaseLabelNames, constLabels,
			), valueType: prometheus.CounterValue,
		},
		blocks: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "database", "blocks_total"),
				"Total number of disk blocks had been accessed by each type of access.",
				[]string{"datname", "access"}, constLabels,
			), valueType: prometheus.CounterValue,
		},
		tuples: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "database", "tuples_total"),
				"Total number of rows processed by each type of operation.",
				[]string{"datname", "op"}, constLabels,
			), valueType: prometheus.CounterValue,
		},
		tempbytes: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "database", "temp_bytes_total"),
				"Total amount of data written to temporary files by queries.",
				databaseLabelNames, constLabels,
			), valueType: prometheus.CounterValue,
		},
		tempfiles: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "database", "temp_files_total"),
				"Total number of temporary files created by queries.",
				databaseLabelNames, constLabels,
			), valueType: prometheus.CounterValue,
		},
		blockstime: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "database", "blk_time_seconds"),
				"Time spent accessing data file blocks by backends in this database in each access type, in seconds.",
				[]string{"datname", "type"}, constLabels,
			), valueType: prometheus.CounterValue, factor: .001,
		},
		sizes: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "database", "size_bytes"),
				"Total size of the database, in bytes.",
				databaseLabelNames, constLabels,
			), valueType: prometheus.GaugeValue,
		},
		statsage: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "database", "stats_age_seconds"),
				"The age of the activity statistics, in seconds.",
				databaseLabelNames, constLabels,
			), valueType: prometheus.CounterValue,
		},
		xidlimit: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "xacts", "left_before_wraparound"),
				"The least number of transactions (among all databases) left before force shutdown due to XID wraparound.",
				[]string{"xid"}, constLabels,
			), valueType: prometheus.CounterValue,
		},
		custom: newDeskSetsFromSubsystems("postgres", settings.Subsystems, constLabels),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresDatabasesCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}

	res, err := conn.Query(databaseQuery)
	if err != nil {
		return err
	}

	stats := parsePostgresDatabasesStats(res, c.labelNames)

	res, err = conn.Query(xidLimitQuery)
	if err != nil {
		return err
	}

	conn.Close()

	xidStats := parsePostgresXidLimitStats(res)

	for _, stat := range stats {
		ch <- c.commits.mustNewConstMetric(stat.xactcommit, stat.datname)
		ch <- c.rollbacks.mustNewConstMetric(stat.xactrollback, stat.datname)
		ch <- c.conflicts.mustNewConstMetric(stat.conflicts, stat.datname)
		ch <- c.deadlocks.mustNewConstMetric(stat.deadlocks, stat.datname)
		ch <- c.blocks.mustNewConstMetric(stat.blksread, stat.datname, "read")
		ch <- c.blocks.mustNewConstMetric(stat.blkshit, stat.datname, "hit")
		ch <- c.tuples.mustNewConstMetric(stat.tupreturned, stat.datname, "returned")
		ch <- c.tuples.mustNewConstMetric(stat.tupfetched, stat.datname, "fetched")
		ch <- c.tuples.mustNewConstMetric(stat.tupinserted, stat.datname, "inserted")
		ch <- c.tuples.mustNewConstMetric(stat.tupupdated, stat.datname, "updated")
		ch <- c.tuples.mustNewConstMetric(stat.tupdeleted, stat.datname, "deleted")

		ch <- c.tempbytes.mustNewConstMetric(stat.tempbytes, stat.datname)
		ch <- c.tempfiles.mustNewConstMetric(stat.tempfiles, stat.datname)
		ch <- c.blockstime.mustNewConstMetric(stat.blkreadtime, stat.datname, "read")
		ch <- c.blockstime.mustNewConstMetric(stat.blkwritetime, stat.datname, "write")
		ch <- c.sizes.mustNewConstMetric(stat.sizebytes, stat.datname)
		ch <- c.statsage.mustNewConstMetric(stat.statsage, stat.datname)
	}

	ch <- c.xidlimit.mustNewConstMetric(xidStats.database, "pg_database")
	ch <- c.xidlimit.mustNewConstMetric(xidStats.prepared, "pg_prepared_xacts")
	ch <- c.xidlimit.mustNewConstMetric(xidStats.replSlot, "pg_replication_slots")

	// Update user-defined metrics.
	err = updateAllDescSets(config, c.custom, ch)
	if err != nil {
		return err
	}

	return nil
}

// postgresDatabaseStat represents per-database stats based on pg_stat_database.
type postgresDatabaseStat struct {
	datname      string
	xactcommit   float64
	xactrollback float64
	blksread     float64
	blkshit      float64
	tupreturned  float64
	tupfetched   float64
	tupinserted  float64
	tupupdated   float64
	tupdeleted   float64
	conflicts    float64
	tempfiles    float64
	tempbytes    float64
	deadlocks    float64
	blkreadtime  float64
	blkwritetime float64
	sizebytes    float64
	statsage     float64
}

// parsePostgresDatabasesStats parses PGResult, extract data and return struct with stats values.
func parsePostgresDatabasesStats(r *model.PGResult, labelNames []string) map[string]postgresDatabaseStat {
	log.Debug("parse postgres database stats")

	var stats = make(map[string]postgresDatabaseStat)

	// process row by row
	for _, row := range r.Rows {
		stat := postgresDatabaseStat{}

		// collect label values
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "datname":
				stat.datname = row[i].String
			}
		}

		// Define a map key as a database name.
		databaseFQName := stat.datname

		// Put stats with labels (but with no data values yet) into stats store.
		stats[databaseFQName] = stat

		// fetch data values from columns
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

			// Run column-specific logic
			switch string(colname.Name) {
			case "xact_commit":
				s := stats[databaseFQName]
				s.xactcommit = v
				stats[databaseFQName] = s
			case "xact_rollback":
				s := stats[databaseFQName]
				s.xactrollback = v
				stats[databaseFQName] = s
			case "blks_read":
				s := stats[databaseFQName]
				s.blksread = v
				stats[databaseFQName] = s
			case "blks_hit":
				s := stats[databaseFQName]
				s.blkshit = v
				stats[databaseFQName] = s
			case "tup_returned":
				s := stats[databaseFQName]
				s.tupreturned = v
				stats[databaseFQName] = s
			case "tup_fetched":
				s := stats[databaseFQName]
				s.tupfetched = v
				stats[databaseFQName] = s
			case "tup_inserted":
				s := stats[databaseFQName]
				s.tupinserted = v
				stats[databaseFQName] = s
			case "tup_updated":
				s := stats[databaseFQName]
				s.tupupdated = v
				stats[databaseFQName] = s
			case "tup_deleted":
				s := stats[databaseFQName]
				s.tupdeleted = v
				stats[databaseFQName] = s
			case "conflicts":
				s := stats[databaseFQName]
				s.conflicts = v
				stats[databaseFQName] = s
			case "temp_files":
				s := stats[databaseFQName]
				s.tempfiles = v
				stats[databaseFQName] = s
			case "temp_bytes":
				s := stats[databaseFQName]
				s.tempbytes = v
				stats[databaseFQName] = s
			case "deadlocks":
				s := stats[databaseFQName]
				s.deadlocks = v
				stats[databaseFQName] = s
			case "blk_read_time":
				s := stats[databaseFQName]
				s.blkreadtime = v
				stats[databaseFQName] = s
			case "blk_write_time":
				s := stats[databaseFQName]
				s.blkwritetime = v
				stats[databaseFQName] = s
			case "size_bytes":
				s := stats[databaseFQName]
				s.sizebytes = v
				stats[databaseFQName] = s
			case "stats_age_seconds":
				s := stats[databaseFQName]
				s.statsage = v
				stats[databaseFQName] = s
			default:
				log.Debugf("unsupported pg_stat_database stat column: %s, skip", string(colname.Name))
				continue
			}
		}
	}

	return stats
}

// xidLimitStats describes how many XIDs left before force database shutdown due to XID wraparound.
type xidLimitStats struct {
	database float64 // based on pg_database.datfrozenxid and datminmxid
	prepared float64 // based on pg_prepared_xacts.transaction
	replSlot float64 // based on pg_replication_slots.xmin and catalog_xmin
}

// parsePostgresXidLimitStats parses database response and returns xidLimitStats.
func parsePostgresXidLimitStats(r *model.PGResult) xidLimitStats {
	log.Debug("parse postgres xid limit stats")

	var stats xidLimitStats

	// process row by row
	for _, row := range r.Rows {
		// Get data value and convert it to float64 used by Prometheus.
		value, err := strconv.ParseFloat(row[1].String, 64)
		if err != nil {
			log.Errorf("invalid input, parse '%s' failed: %s; skip", row[1].String, err)
			continue
		}

		switch row[0].String {
		case "database":
			stats.database = value
		case "prepared_xacts":
			stats.prepared = value
		case "replication_slots":
			stats.replSlot = value
		}
	}

	return stats
}
