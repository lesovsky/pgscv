package collector

import (
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"strings"
)

const (
	// admin console query used for retrieving pools stats
	poolQuery = "SHOW POOLS"
)

type pgbouncerPoolsCollector struct {
	labelNames []string
	conns      typedDesc
	maxwait    typedDesc
}

// NewPgbouncerPoolsCollector returns a new Collector exposing pgbouncer pools connections usage stats.
// For details see https://www.pgbouncer.org/usage.html#show-pools.
func NewPgbouncerPoolsCollector(constLabels prometheus.Labels) (Collector, error) {
	var poolsLabelNames = []string{"database", "user", "pool_mode", "state"}

	return &pgbouncerPoolsCollector{
		conns: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("pgbouncer", "pool", "connections_in_flight"),
				"The total number of connections established by each state.",
				poolsLabelNames, constLabels,
			),
			valueType: prometheus.GaugeValue,
		},
		maxwait: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("pgbouncer", "pool", "max_wait_seconds"),
				"Total time the first (oldest) client in the queue has waited, in seconds.",
				[]string{"database", "user", "pool_mode"}, constLabels,
			),
			valueType: prometheus.GaugeValue,
		},
		labelNames: poolsLabelNames,
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *pgbouncerPoolsCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	res, err := conn.Query(poolQuery)
	if err != nil {
		return err
	}

	stats := parsePgbouncerPoolsStats(res, c.labelNames)

	for _, stat := range stats {
		ch <- c.conns.mustNewConstMetric(stat.clActive, stat.database, stat.user, stat.mode, "cl_active")
		ch <- c.conns.mustNewConstMetric(stat.clWaiting, stat.database, stat.user, stat.mode, "cl_waiting")
		ch <- c.conns.mustNewConstMetric(stat.svActive, stat.database, stat.user, stat.mode, "sv_active")
		ch <- c.conns.mustNewConstMetric(stat.svIdle, stat.database, stat.user, stat.mode, "sv_idle")
		ch <- c.conns.mustNewConstMetric(stat.svUsed, stat.database, stat.user, stat.mode, "sv_used")
		ch <- c.conns.mustNewConstMetric(stat.svTested, stat.database, stat.user, stat.mode, "sv_tested")
		ch <- c.conns.mustNewConstMetric(stat.svLogin, stat.database, stat.user, stat.mode, "sv_login")
		ch <- c.maxwait.mustNewConstMetric(stat.maxWait, stat.database, stat.user, stat.mode)
	}

	return nil
}

// pgbouncerPoolStat is a per-pool store for connections metrics.
type pgbouncerPoolStat struct {
	database  string
	user      string
	mode      string
	clActive  float64
	clWaiting float64
	svActive  float64
	svIdle    float64
	svUsed    float64
	svTested  float64
	svLogin   float64
	maxWait   float64
}

func parsePgbouncerPoolsStats(r *model.PGResult, labelNames []string) map[string]pgbouncerPoolStat {
	var stats = map[string]pgbouncerPoolStat{}

	for _, row := range r.Rows {
		stat := pgbouncerPoolStat{}

		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "database":
				stat.database = row[i].String
			case "user":
				stat.user = row[i].String
			case "pool_mode":
				stat.mode = row[i].String
			}
		}

		// create a pool name consisting of trio database/user/pool_mode
		poolname := strings.Join([]string{stat.database, stat.user, stat.mode}, "/")

		stats[poolname] = stat

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

				// Update stats struct
				switch string(colname.Name) {
				case "cl_active":
					s := stats[poolname]
					s.clActive = v
					stats[poolname] = s
				case "cl_waiting":
					s := stats[poolname]
					s.clWaiting = v
					stats[poolname] = s
				case "sv_active":
					s := stats[poolname]
					s.svActive = v
					stats[poolname] = s
				case "sv_idle":
					s := stats[poolname]
					s.svIdle = v
					stats[poolname] = s
				case "sv_used":
					s := stats[poolname]
					s.svUsed = v
					stats[poolname] = s
				case "sv_tested":
					s := stats[poolname]
					s.svTested = v
					stats[poolname] = s
				case "sv_login":
					s := stats[poolname]
					s.svLogin = v
					stats[poolname] = s
				case "maxwait":
					s := stats[poolname]
					s.maxWait = v
					stats[poolname] = s
				default:
					log.Debugf("unsupported 'SHOW POOLS' stat column: %s, skip", string(colname.Name))
					continue
				}
			}
		}
	}

	return stats
}
