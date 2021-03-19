package collector

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"strconv"
)

const walArchivingQuery = "SELECT " +
	"archived_count, failed_count, extract(epoch from now() - last_archived_time) AS since_last_archive_seconds, " +
	"(select name from pg_ls_waldir() order by modification desc limit 1) AS last_modified_wal, last_archived_wal " +
	"FROM pg_stat_archiver WHERE archived_count > 0"

type postgresWalArchivingCollector struct {
	archived             typedDesc
	failed               typedDesc
	sinceArchivedSeconds typedDesc
	archivingLag         typedDesc
}

// NewPostgresWalArchivingCollector returns a new Collector exposing postgres WAL archiving stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ARCHIVER-VIEW
func NewPostgresWalArchivingCollector(constLabels prometheus.Labels) (Collector, error) {
	return &postgresWalArchivingCollector{
		archived: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "archiver", "archived_total"),
				"Total number of WAL segments had been successfully archived.",
				nil, constLabels,
			), valueType: prometheus.CounterValue,
		},
		failed: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "archiver", "failed_total"),
				"Total number of attempts when WAL segments had been failed to archive.",
				nil, constLabels,
			), valueType: prometheus.CounterValue,
		},
		sinceArchivedSeconds: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "archiver", "since_last_archive_seconds"),
				"Number of seconds since last WAL segment had been successfully archived.",
				nil, constLabels,
			), valueType: prometheus.GaugeValue,
		},
		archivingLag: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "archiver", "lag_bytes"),
				"Amount of WAL segments ready, but not archived, in bytes.",
				nil, constLabels,
			), valueType: prometheus.GaugeValue,
		},
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresWalArchivingCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	res, err := conn.Query(walArchivingQuery)
	if err != nil {
		return err
	}

	stats := parsePostgresWalArchivingStats(res)

	if stats.archived == 0 {
		log.Debugln("zero archived WAL segments, skip collecting archiver stats")
		return nil
	}

	ch <- c.archived.mustNewConstMetric(stats.archived)
	ch <- c.failed.mustNewConstMetric(stats.failed)
	ch <- c.sinceArchivedSeconds.mustNewConstMetric(stats.sinceArchivedSeconds)

	lag, err := countWalArchivingLag(stats.segLastModified, stats.segLastArchived, config.WalSegmentSize)
	if err != nil {
		return err
	}

	ch <- c.archivingLag.mustNewConstMetric(lag)

	return nil
}

// postgresWalArchivingStat describes stats about WAL archiving.
type postgresWalArchivingStat struct {
	archived             float64
	failed               float64
	sinceArchivedSeconds float64
	segLastArchived      string
	segLastModified      string
}

// parsePostgresWalArchivingStats parses PGResult, extract data and return struct with stats values.
func parsePostgresWalArchivingStats(r *model.PGResult) postgresWalArchivingStat {
	log.Debug("parse postgres WAL archiving stats")

	var stats postgresWalArchivingStat

	// process row by row
	for _, row := range r.Rows {
		// collect non-numeric values
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "last_modified_wal":
				stats.segLastModified = row[i].String
			case "last_archived_wal":
				stats.segLastArchived = row[i].String
			}
		}

		for i, colname := range r.Colnames {
			// Skip empty (NULL) values.
			if !row[i].Valid {
				continue
			}

			// Skip columns with WAL segments names.
			if stringsContains([]string{"last_modified_wal", "last_archived_wal"}, string(colname.Name)) {
				log.Debugf("skip label mapped column '%s'", string(colname.Name))
				continue
			}

			// Get data value and convert it to float64 used by Prometheus.
			v, err := strconv.ParseFloat(row[i].String, 64)
			if err != nil {
				log.Errorf("invalid input, parse '%s' failed: %s; skip", row[i].String, err)
				continue
			}

			// Update stats struct
			switch string(colname.Name) {
			case "archived_count":
				stats.archived = v
			case "failed_count":
				stats.failed = v
			case "since_last_archive_seconds":
				stats.sinceArchivedSeconds = v
			default:
				log.Debugf("unsupported pg_stat_archiver stat column: %s, skip", string(colname.Name))
				continue
			}
		}
	}

	return stats
}

// countWalArchivingLag counts archiving lag between two WAL segments.
func countWalArchivingLag(segLastModified string, segLastArchived string, walSegSize uint64) (float64, error) {
	currentSegNo, err := parseWalFileName(segLastModified, walSegSize)
	if err != nil {
		return 0, err
	}

	archivedSegNo, err := parseWalFileName(segLastArchived, walSegSize)
	if err != nil {
		return 0, err
	}

	lag := (currentSegNo - (archivedSegNo + 1)) * walSegSize

	return float64(lag), nil
}

// parseWalFileName return the number of segment since DATADIR initialization accordingly to its file name.
func parseWalFileName(name string, walSegSize uint64) (uint64, error) {
	if len(name) != 24 {
		return 0, fmt.Errorf("invalid input: wrong WAL segment name '%s'", name)
	}

	// WAL segment size can range from 1MB to 1GB. See WalSegMinSize/WalSegMaxSize macros in xlog_internal.h around line 86
	if walSegSize < 1024*1024 || walSegSize > 1024*1024*1024 {
		return 0, fmt.Errorf("invalid input: wrong WAL segment size '%s'", name)
	}

	// Get number using high number of segment
	logSegNoHi, err := strconv.ParseUint(name[8:16], 0x10, 32)
	if err != nil {
		return 0, err
	}

	logSegNoLo, err := strconv.ParseUint(name[16:24], 0x10, 32)
	if err != nil {
		return 0, err
	}

	// Number of low segments per single high segment depends on used WAL segment size.
	// For example:
	//   with 16MB it is 256 segments from 00000000 to 000000FF
	//   with 64MB it is 16 segments from 00000000 to 0000003F
	walSegPerHi := 0x100000000 / walSegSize

	// Low number must not exceed the total number of segments per high number.
	// For details see XLogSegmentsPerXLogId macro in xlog_internal.h around line 98.
	if logSegNoLo > walSegPerHi {
		return 0, fmt.Errorf("invalid low number in WAL segment '%s'", name)
	}

	// Calculate the number of segment since DATADIR initialization.
	logSegNo := (logSegNoHi * walSegPerHi) + logSegNoLo

	return logSegNo, nil
}
