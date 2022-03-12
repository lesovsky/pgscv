package collector

import (
	"github.com/lesovsky/pgscv/internal/log"
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/lesovsky/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"strings"
)

const (
	// Query for Postgres version 9.6 and older.
	postgresReplicationSlotQuery96 = "SELECT database, slot_name, slot_type, active, pg_current_xlog_location() - restart_lsn AS since_restart_bytes FROM pg_replication_slots"

	// Query for Postgres versions from 10 and newer.
	postgresReplicationSlotQueryLatest = "SELECT database, slot_name, slot_type, active, pg_current_wal_lsn() - restart_lsn AS since_restart_bytes FROM pg_replication_slots"
)

//
type postgresReplicationSlotCollector struct {
	restart typedDesc
}

// NewPostgresReplicationSlotsCollector returns a new Collector exposing postgres replication slots stats.
// For details see https://www.postgresql.org/docs/current/view-pg-replication-slots.html
func NewPostgresReplicationSlotsCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &postgresReplicationSlotCollector{
		restart: newBuiltinTypedDesc(
			descOpts{"postgres", "replication_slot", "wal_retain_bytes", "Number of WAL retained and required by consumers, in bytes.", 0},
			prometheus.GaugeValue,
			[]string{"database", "slot_name", "slot_type", "active"}, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresReplicationSlotCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	res, err := conn.Query(selectReplicationSlotQuery(config.serverVersionNum))
	if err != nil {
		return err
	}

	// parse pg_stat_statements stats
	stats := parsePostgresReplicationSlotStats(res, c.restart.labelNames)

	for _, stat := range stats {
		ch <- c.restart.newConstMetric(stat.retainedBytes, stat.database, stat.slotname, stat.slottype, stat.active)
	}

	return nil
}

// postgresReplicationSlotStat represents per-slot stats based on pg_replication_slots.
type postgresReplicationSlotStat struct {
	database      string
	slotname      string
	slottype      string
	active        string
	retainedBytes float64
}

// parsePostgresReplicationSlotStats parses PGResult and returns struct with stats values.
func parsePostgresReplicationSlotStats(r *model.PGResult, labelNames []string) map[string]postgresReplicationSlotStat {
	log.Debug("parse postgres replication slots stats")

	var stats = make(map[string]postgresReplicationSlotStat)

	for _, row := range r.Rows {
		stat := postgresReplicationSlotStat{}

		// collect label values
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "database":
				stat.database = row[i].String
			case "slot_name":
				stat.slotname = row[i].String
			case "slot_type":
				stat.slottype = row[i].String
			case "active":
				stat.active = row[i].String
			}
		}

		// use pid as key in the map
		slotFQName := strings.Join([]string{stat.database, stat.slotname, stat.slottype}, "/")

		// Put stats with labels (but with no data values yet) into stats store.
		stats[slotFQName] = stat

		// fetch data values from columns
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

			s := stats[slotFQName]

			// Run column-specific logic
			switch string(colname.Name) {
			case "since_restart_bytes":
				s.retainedBytes = v
			default:
				continue
			}

			stats[slotFQName] = s
		}
	}

	return stats
}

// selectReplicationQuery returns suitable replication query depending on passed version.
func selectReplicationSlotQuery(version int) string {
	switch {
	case version < PostgresV10:
		return postgresReplicationSlotQuery96
	default:
		return postgresReplicationSlotQueryLatest
	}
}
