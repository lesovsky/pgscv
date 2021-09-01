package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"strconv"
)

const (
	postgresWalQuery96 = "SELECT pg_is_in_recovery()::int AS recovery, " +
		"(case pg_is_in_recovery() when 't' then pg_last_xlog_receive_location() else pg_current_xlog_location() end) - '0/00000000' AS wal_bytes"

	postgresWalQuery13 = "SELECT pg_is_in_recovery()::int AS recovery, " +
		"(case pg_is_in_recovery() when 't' then pg_last_wal_receive_lsn() else pg_current_wal_lsn() end) - '0/00000000' AS wal_bytes"

	postgresWalQueryLatest = "SELECT pg_is_in_recovery()::int AS recovery, wal_records, wal_fpi, " +
		"(case pg_is_in_recovery() when 't' then pg_last_wal_receive_lsn() - '0/00000000' else wal_bytes end) AS wal_bytes, " +
		"wal_buffers_full, wal_write, wal_sync, wal_write_time, wal_sync_time, " +
		"extract('epoch' from stats_reset) as reset_time " +
		"FROM pg_stat_wal"
)

type postgresWalCollector struct {
	recovery        typedDesc
	records         typedDesc
	writtenAllBytes typedDesc
	writtenBytes    typedDesc
	buffersFull     typedDesc
	writes          typedDesc
	syncs           typedDesc
	secondsAll      typedDesc
	seconds         typedDesc
	resetUnix       typedDesc
}

// NewPostgresWalCollector returns a new Collector exposing postgres WAL stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-WAL-VIEW
func NewPostgresWalCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &postgresWalCollector{
		recovery: newBuiltinTypedDesc(
			descOpts{"postgres", "recovery", "info", "Current recovery state, 0 - not in recovery; 1 - in recovery.", 0},
			prometheus.GaugeValue,
			nil, constLabels,
			settings.Filters,
		),
		records: newBuiltinTypedDesc(
			descOpts{"postgres", "wal", "written_records_total", "Total amount of WAL records written (zero in case of standby).", 0},
			prometheus.CounterValue,
			nil, constLabels,
			settings.Filters,
		),
		writtenAllBytes: newBuiltinTypedDesc(
			descOpts{"postgres", "wal", "written_bytes_all_total", "Total amount of WAL written (or received in case of standby), in bytes.", 0},
			prometheus.CounterValue,
			nil, constLabels,
			settings.Filters,
		),
		writtenBytes: newBuiltinTypedDesc(
			descOpts{"postgres", "wal", "written_bytes_total", "Total amount of WAL written by each type of WAL (zero in case of standby), in bytes.", 0},
			prometheus.CounterValue,
			[]string{"wal"}, constLabels,
			settings.Filters,
		),
		buffersFull: newBuiltinTypedDesc(
			descOpts{"postgres", "wal", "buffers_full_total", "Total number of times WAL data was written to disk because WAL buffers became full (zero in case of standby).", 0},
			prometheus.CounterValue,
			nil, constLabels,
			settings.Filters,
		),
		writes: newBuiltinTypedDesc(
			descOpts{"postgres", "wal", "write_total", "Total number of times WAL buffers were written out to disk via XLogWrite request (zero in case of standby).", 0},
			prometheus.CounterValue,
			nil, constLabels,
			settings.Filters,
		),
		syncs: newBuiltinTypedDesc(
			descOpts{"postgres", "wal", "sync_total", "Total number of times WAL files were synced to disk via issue_xlog_fsync request (zero in case of standby).", 0},
			prometheus.CounterValue,
			nil, constLabels,
			settings.Filters,
		),
		secondsAll: newBuiltinTypedDesc(
			descOpts{"postgres", "wal", "seconds_all_total", "Total amount of time spent processing WAL buffers (zero in case of standby), in seconds.", .001},
			prometheus.CounterValue,
			nil, constLabels,
			settings.Filters,
		),
		seconds: newBuiltinTypedDesc(
			descOpts{"postgres", "wal", "seconds_total", "Total amount of time spent processing WAL buffers by each operation (zero in case of standby), in seconds.", .001},
			prometheus.CounterValue,
			[]string{"op"}, constLabels,
			settings.Filters,
		),
		resetUnix: newBuiltinTypedDesc(
			descOpts{"postgres", "wal", "stats_reset_time", "Time at which WAL statistics were last reset, in unixtime.", 0},
			prometheus.CounterValue,
			nil, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresWalCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Get WAL usage stats.
	res, err := conn.Query(selectWalQuery(config.serverVersionNum))
	if err != nil {
		return err
	}

	stats := parsePostgresWalStats(res)

	for k, v := range stats {
		switch k {
		case "recovery":
			ch <- c.recovery.newConstMetric(v)
		case "wal_records":
			ch <- c.records.newConstMetric(v)
		case "wal_fpi":
			ch <- c.writtenBytes.newConstMetric(v*float64(config.walBlockSize), "fpi")
		case "wal_bytes":
			ch <- c.writtenAllBytes.newConstMetric(v)
		case "wal_buffers_full":
			ch <- c.buffersFull.newConstMetric(v)
		case "wal_write":
			ch <- c.writes.newConstMetric(v)
		case "wal_sync":
			ch <- c.syncs.newConstMetric(v)
		case "wal_write_time":
			ch <- c.seconds.newConstMetric(v, "write")
		case "wal_sync_time":
			ch <- c.seconds.newConstMetric(v, "sync")
		case "wal_all_time":
			ch <- c.secondsAll.newConstMetric(v)
		case "reset_time":
			ch <- c.resetUnix.newConstMetric(v)
		default:
			continue
		}
	}

	// Collect WAL bytes of regular (non-FPI) records
	if fpi, ok := stats["wal_fpi"]; ok {
		ch <- c.writtenBytes.newConstMetric(stats["wal_bytes"]-(fpi*float64(config.walBlockSize)), "regular")
	}

	return nil
}

// parsePostgresWalStats parses PGResult and returns struct with data values
func parsePostgresWalStats(r *model.PGResult) map[string]float64 {
	log.Debug("parse postgres WAL stats")

	stats := map[string]float64{}

	for _, row := range r.Rows {
		for i, colname := range r.Colnames {
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

			// Column name used as a key.
			stats[string(colname.Name)] = v
		}
	}

	// Count total time spent on WAL buffers processing.
	wTime, ok1 := stats["wal_write_time"]
	sTime, ok2 := stats["wal_sync_time"]
	if ok1 && ok2 {
		stats["wal_all_time"] = wTime + sTime
	}

	return stats
}

// selectWalQuery returns suitable wal state query depending on passed version.
func selectWalQuery(version int) string {
	switch {
	case version < PostgresV10:
		return postgresWalQuery96
	case version < PostgresV14:
		return postgresWalQuery13
	default:
		return postgresWalQueryLatest
	}
}
