package collector

import (
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
)

const (
	postgresTempFilesInflightQuery = `WITH RECURSIVE tablespace_dirs AS (
    SELECT dirname, 'pg_tblspc/' || dirname || '/' AS path, 1 AS depth FROM pg_catalog.pg_ls_dir('pg_tblspc/', true, false) AS dirname
    UNION ALL
    SELECT subdir, td.path || subdir || '/', td.depth + 1 FROM tablespace_dirs AS td, pg_catalog.pg_ls_dir(td.path, true, false) AS subdir WHERE td.depth < 3
), temp_dirs AS (
    SELECT td.path, ts.spcname AS tablespace
        FROM tablespace_dirs AS td
        INNER JOIN pg_catalog.pg_tablespace AS ts ON (ts.oid = substring(td.path FROM 'pg_tblspc/(\d+)')::int)
        WHERE td.depth = 3 AND td.dirname = 'pgsql_tmp'
    UNION ALL
    VALUES ('base/pgsql_tmp/', 'pg_default')
), temp_files AS (
    SELECT td.tablespace, pg_stat_file(td.path || '/' || filename, true) AS file_stat
    FROM temp_dirs AS td
    LEFT JOIN pg_catalog.pg_ls_dir(td.path, true, false) AS filename ON true
) SELECT tablespace,
    count((file_stat).size) AS files_total,
    coalesce(sum((file_stat).size)::BIGINT, 0) AS bytes_total,
    coalesce(extract(epoch from clock_timestamp() - min((file_stat).access)), 0) AS max_age_seconds
FROM temp_files GROUP BY 1`
)

type postgresStorageCollector struct {
	tempFiles       typedDesc
	tempFilesBytes  typedDesc
	tempFilesMaxAge typedDesc
}

// NewPostgresStorageCollector returns a new Collector exposing various stats related to Postgres storage layer.
// This stats observed using different stats sources.
func NewPostgresStorageCollector(constLabels prometheus.Labels) (Collector, error) {
	return &postgresStorageCollector{
		tempFiles: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "temp_files", "in_flight"),
				"Total number of temporary files processed in flight.",
				[]string{"tablespace"}, constLabels,
			), valueType: prometheus.GaugeValue,
		},
		tempFilesBytes: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "temp_bytes", "in_flight"),
				"Total number of disk space occupied by temp files in flight.",
				[]string{"tablespace"}, constLabels,
			), valueType: prometheus.GaugeValue,
		},
		tempFilesMaxAge: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "temp_files", "max_age_seconds"),
				"The age of the oldest temporary file, in seconds.",
				[]string{"tablespace"}, constLabels,
			), valueType: prometheus.GaugeValue,
		},
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresStorageCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	res, err := conn.Query(postgresTempFilesInflightQuery)
	if err != nil {
		return err
	}

	stats := parsePostgresTempFileInflght(res)

	for _, stat := range stats {
		ch <- c.tempFiles.mustNewConstMetric(stat.tempfiles, stat.tablespace)
		ch <- c.tempFilesBytes.mustNewConstMetric(stat.tempbytes, stat.tablespace)
		ch <- c.tempFilesMaxAge.mustNewConstMetric(stat.tempmaxage, stat.tablespace)
	}

	return nil
}

// postgresConflictStat represents per-database recovery conflicts stats based on pg_stat_database_conflicts.
type postgresTempfilesStat struct {
	tablespace string
	tempfiles  float64
	tempbytes  float64
	tempmaxage float64
}

// parsePostgresTempFileInflght parses PGResult, extract data and return struct with stats values.
func parsePostgresTempFileInflght(r *model.PGResult) map[string]postgresTempfilesStat {
	var stats = make(map[string]postgresTempfilesStat)

	// process row by row
	for _, row := range r.Rows {
		stat := postgresTempfilesStat{}

		// collect label values
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "tablespace":
				stat.tablespace = row[i].String
			}
		}

		// Define a map key as a tablespace name.
		tablespaceFQName := stat.tablespace

		// Put stats with labels (but with no data values yet) into stats store.
		stats[tablespaceFQName] = stat

		// fetch data values from columns
		for i, colname := range r.Colnames {
			// skip tablespace column - it's mapped as a label
			if string(colname.Name) == "tablespace" {
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
			case "files_total":
				s := stats[tablespaceFQName]
				s.tempfiles = v
				stats[tablespaceFQName] = s
			case "bytes_total":
				s := stats[tablespaceFQName]
				s.tempbytes = v
				stats[tablespaceFQName] = s
			case "max_age_seconds":
				s := stats[tablespaceFQName]
				s.tempmaxage = v
				stats[tablespaceFQName] = s
			default:
				log.Debugf("unsupported stat column: %s, skip", string(colname.Name))
				continue
			}
		}
	}

	return stats
}
