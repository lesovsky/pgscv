package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"strconv"
)

const (
	postgresLocksQuery = "SELECT mode, count(*) FROM pg_locks GROUP BY mode"
)

// postgresLocksCollector is a collector with locks related metrics descriptors.
type postgresLocksCollector struct {
	modes typedDesc
}

// NewPostgresLocksCollector creates new postgresLocksCollector.
func NewPostgresLocksCollector(constLabels prometheus.Labels, _ model.CollectorSettings) (Collector, error) {
	return &postgresLocksCollector{
		modes: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "locks", "in_flight"),
				"Number of in-flight locks held by active processes in each mode.",
				[]string{"mode"}, constLabels,
			), valueType: prometheus.GaugeValue,
		},
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
	res, err := conn.Query(postgresLocksQuery)
	if err != nil {
		return err
	}

	// parse pg_stat_activity stats
	stats := parsePostgresLocksStats(res)

	for mode, value := range stats {
		ch <- c.modes.mustNewConstMetric(value, mode)
	}

	return nil
}

// parsePostgresLocksStats parses result returned from Postgres and return stats map.
func parsePostgresLocksStats(r *model.PGResult) map[string]float64 {
	log.Debug("parse postgres locks stats")

	stats := map[string]float64{
		"AccessShareLock":          0,
		"RowShareLock":             0,
		"RowExclusiveLock":         0,
		"ShareUpdateExclusiveLock": 0,
		"ShareLock":                0,
		"ShareRowExclusiveLock":    0,
		"ExclusiveLock":            0,
		"AccessExclusiveLock":      0,
	}

	for _, row := range r.Rows {
		if len(row) != 2 {
			log.Warn("invalid input: wrong number of columns, skip")
		}

		mode := row[0].String

		v, err := strconv.ParseFloat(row[1].String, 64)
		if err != nil {
			log.Errorf("invalid input, parse '%s' failed: %s; skip", row[1].String, err)
			continue
		}

		stats[mode] = v
	}

	return stats
}
