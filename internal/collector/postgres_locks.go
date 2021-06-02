package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"strconv"
)

const (
	locksQuery = "SELECT " +
		"count(*) FILTER (WHERE mode = 'AccessShareLock') AS access_share_lock, " +
		"count(*) FILTER (WHERE mode = 'RowShareLock') AS row_share_lock, " +
		"count(*) FILTER (WHERE mode = 'RowExclusiveLock') AS row_exclusive_lock, " +
		"count(*) FILTER (WHERE mode = 'ShareUpdateExclusiveLock') AS share_update_exclusive_lock, " +
		"count(*) FILTER (WHERE mode = 'ShareLock') AS share_lock, " +
		"count(*) FILTER (WHERE mode = 'ShareRowExclusiveLock') AS share_row_exclusive_lock, " +
		"count(*) FILTER (WHERE mode = 'ExclusiveLock') AS exclusive_lock, " +
		"count(*) FILTER (WHERE mode = 'AccessExclusiveLock') AS access_exclusive_lock, " +
		"count(*) FILTER (WHERE not granted) AS not_granted, " +
		"count(*) AS total " +
		"FROM pg_locks"
)

// postgresLocksCollector is a collector with locks related metrics descriptors.
type postgresLocksCollector struct {
	locks      typedDesc
	locksAll   typedDesc
	notgranted typedDesc
}

// NewPostgresLocksCollector creates new postgresLocksCollector.
func NewPostgresLocksCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &postgresLocksCollector{
		locks: newBuiltinTypedDesc(
			descOpts{"postgres", "locks", "in_flight", "Number of in-flight locks held by active processes in each mode.", 0},
			prometheus.GaugeValue,
			[]string{"mode"}, constLabels,
			settings.Filters,
		),
		locksAll: newBuiltinTypedDesc(
			descOpts{"postgres", "locks", "all_in_flight", "Total number of all in-flight locks held by active processes.", 0},
			prometheus.GaugeValue,
			nil, constLabels,
			settings.Filters,
		),
		notgranted: newBuiltinTypedDesc(
			descOpts{"postgres", "locks", "not_granted_in_flight", "Number of in-flight not granted locks held by active processes.", 0},
			prometheus.GaugeValue,
			nil, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects locks metrics.
func (c *postgresLocksCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	// get pg_stat_activity stats
	res, err := conn.Query(locksQuery)
	if err != nil {
		return err
	}

	// parse pg_stat_activity stats
	stats := parsePostgresLocksStats(res)

	ch <- c.locks.newConstMetric(stats.accessShareLock, "AccessShareLock")
	ch <- c.locks.newConstMetric(stats.rowShareLock, "RowShareLock")
	ch <- c.locks.newConstMetric(stats.rowExclusiveLock, "RowExclusiveLock")
	ch <- c.locks.newConstMetric(stats.shareUpdateExclusiveLock, "ShareUpdateExclusiveLock")
	ch <- c.locks.newConstMetric(stats.shareLock, "ShareLock")
	ch <- c.locks.newConstMetric(stats.shareRowExclusiveLock, "ShareRowExclusiveLock")
	ch <- c.locks.newConstMetric(stats.exclusiveLock, "ExclusiveLock")
	ch <- c.locks.newConstMetric(stats.accessExclusiveLock, "AccessExclusiveLock")
	ch <- c.notgranted.newConstMetric(stats.notGranted)
	ch <- c.locksAll.newConstMetric(stats.total)

	return nil
}

// locksStat describes locks statistics.
type locksStat struct {
	accessShareLock          float64
	rowShareLock             float64
	rowExclusiveLock         float64
	shareUpdateExclusiveLock float64
	shareLock                float64
	shareRowExclusiveLock    float64
	exclusiveLock            float64
	accessExclusiveLock      float64
	notGranted               float64
	total                    float64
}

// parsePostgresLocksStats parses result returned from Postgres and return locks stats.
func parsePostgresLocksStats(r *model.PGResult) locksStat {
	log.Debug("parse postgres locks stats")

	stats := locksStat{}

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

			// Update stats struct
			switch string(colname.Name) {
			case "access_share_lock":
				stats.accessShareLock = v
			case "row_share_lock":
				stats.rowShareLock = v
			case "row_exclusive_lock":
				stats.rowExclusiveLock = v
			case "share_update_exclusive_lock":
				stats.shareUpdateExclusiveLock = v
			case "share_lock":
				stats.shareLock = v
			case "share_row_exclusive_lock":
				stats.shareRowExclusiveLock = v
			case "exclusive_lock":
				stats.exclusiveLock = v
			case "access_exclusive_lock":
				stats.accessExclusiveLock = v
			case "not_granted":
				stats.notGranted = v
			case "total":
				stats.total = v
			default:
				continue
			}
		}
	}

	return stats
}
