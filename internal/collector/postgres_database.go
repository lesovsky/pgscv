package collector

import (
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
)

const databaseQuery = `SELECT
  COALESCE(datname, '__shared__') AS datname,
  xact_commit, xact_rollback,
  blks_read, blks_hit,
  tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted,
  conflicts, temp_files, temp_bytes, deadlocks,
  blk_read_time, blk_write_time,
  pg_database_size(datname) as size_bytes,
  coalesce(extract('epoch' from age(now(), stats_reset)), 0) as stats_age_seconds
FROM pg_stat_database WHERE datname IN (SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate)`

type postgresDatabasesCollector struct {
	blocks     typedDesc
	tuples     typedDesc
	events     typedDesc
	tempbytes  typedDesc
	blockstime typedDesc
	sizes      typedDesc
	statsage   typedDesc
	labelNames []string
}

// NewPostgresDatabasesCollector returns a new Collector exposing postgres databases stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-DATABASE-VIEW
func NewPostgresDatabasesCollector(constLabels prometheus.Labels) (Collector, error) {
	var databaseLabelNames = []string{"datname"}

	return &postgresDatabasesCollector{
		labelNames: databaseLabelNames,
		events: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "database", "events_total"),
				"Total number of events occurred in this database in each event type.",
				[]string{"datname", "type"}, constLabels,
			), valueType: prometheus.CounterValue,
		},
		blocks: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "database", "blocks_total"),
				"Total number of disk blocks accesses in this database in each access type.",
				[]string{"datname", "access"}, constLabels,
			), valueType: prometheus.CounterValue,
		},
		tuples: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "database", "tuples_total"),
				"Total number of rows processed in this database in each operation type.",
				[]string{"datname", "op"}, constLabels,
			), valueType: prometheus.CounterValue,
		},
		tempbytes: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "database", "temp_bytes_total"),
				"Total amount of data written to temporary files by queries in this database.",
				[]string{"datname"}, constLabels,
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
				prometheus.BuildFQName("postgres", "database", "size_bytes_total"),
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
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresDatabasesCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	res, err := conn.Query(databaseQuery)
	if err != nil {
		return err
	}

	stats := parsePostgresDatabasesStats(res, c.labelNames)

	for _, stat := range stats {
		ch <- c.events.mustNewConstMetric(stat.xactcommit, stat.datname, "xact_commit")
		ch <- c.events.mustNewConstMetric(stat.xactrollback, stat.datname, "xact_rollback")
		ch <- c.events.mustNewConstMetric(stat.conflicts, stat.datname, "conflicts")
		ch <- c.events.mustNewConstMetric(stat.tempfiles, stat.datname, "tempfiles")
		ch <- c.events.mustNewConstMetric(stat.deadlocks, stat.datname, "deadlocks")
		ch <- c.blocks.mustNewConstMetric(stat.blksread, stat.datname, "read")
		ch <- c.blocks.mustNewConstMetric(stat.blkshit, stat.datname, "hit")
		ch <- c.tuples.mustNewConstMetric(stat.tupreturned, stat.datname, "returned")
		ch <- c.tuples.mustNewConstMetric(stat.tupfetched, stat.datname, "fetched")
		ch <- c.tuples.mustNewConstMetric(stat.tupinserted, stat.datname, "inserted")
		ch <- c.tuples.mustNewConstMetric(stat.tupupdated, stat.datname, "updated")
		ch <- c.tuples.mustNewConstMetric(stat.tupdeleted, stat.datname, "deleted")

		ch <- c.tempbytes.mustNewConstMetric(stat.tempbytes, stat.datname)
		ch <- c.blockstime.mustNewConstMetric(stat.blkreadtime, stat.datname, "read")
		ch <- c.blockstime.mustNewConstMetric(stat.blkwritetime, stat.datname, "write")
		ch <- c.sizes.mustNewConstMetric(stat.sizebytes, stat.datname)
		ch <- c.statsage.mustNewConstMetric(stat.statsage, stat.datname)
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
