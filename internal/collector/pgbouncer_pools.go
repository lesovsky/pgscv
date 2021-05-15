package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"strconv"
	"strings"
)

const (
	// admin console queries used for retrieving stats.
	poolsQuery   = "SHOW POOLS"
	clientsQuery = "SHOW CLIENTS"
)

type pgbouncerPoolsCollector struct {
	labelNames []string
	conns      typedDesc
	maxwait    typedDesc
	clients    typedDesc
}

// NewPgbouncerPoolsCollector returns a new Collector exposing pgbouncer pools connections usage stats.
// For details see https://www.pgbouncer.org/usage.html#show-pools.
func NewPgbouncerPoolsCollector(constLabels prometheus.Labels, _ model.CollectorSettings) (Collector, error) {
	var poolsLabelNames = []string{"user", "database", "pool_mode", "state"}

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
				[]string{"user", "database", "pool_mode"}, constLabels,
			),
			valueType: prometheus.GaugeValue,
		},
		clients: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("pgbouncer", "client", "connections_in_flight"),
				"The total number of client connections established by source address.",
				[]string{"user", "database", "address"}, constLabels,
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

	res, err := conn.Query(poolsQuery)
	if err != nil {
		return err
	}

	poolsStats := parsePgbouncerPoolsStats(res, c.labelNames)

	res, err = conn.Query(clientsQuery)
	if err != nil {
		return err
	}

	clientsStats := parsePgbouncerClientsStats(res)

	// Process pools stats.
	for _, stat := range poolsStats {
		ch <- c.conns.mustNewConstMetric(stat.clActive, stat.user, stat.database, stat.mode, "cl_active")
		ch <- c.conns.mustNewConstMetric(stat.clWaiting, stat.user, stat.database, stat.mode, "cl_waiting")
		ch <- c.conns.mustNewConstMetric(stat.svActive, stat.user, stat.database, stat.mode, "sv_active")
		ch <- c.conns.mustNewConstMetric(stat.svIdle, stat.user, stat.database, stat.mode, "sv_idle")
		ch <- c.conns.mustNewConstMetric(stat.svUsed, stat.user, stat.database, stat.mode, "sv_used")
		ch <- c.conns.mustNewConstMetric(stat.svTested, stat.user, stat.database, stat.mode, "sv_tested")
		ch <- c.conns.mustNewConstMetric(stat.svLogin, stat.user, stat.database, stat.mode, "sv_login")
		ch <- c.maxwait.mustNewConstMetric(stat.maxWait, stat.user, stat.database, stat.mode)
	}

	// Process client connections stats.
	for k, v := range clientsStats {
		vals := strings.Split(k, "/")
		if len(vals) != 3 {
			log.Warnf("invalid number of values in client connections stats: must 3, got %d; skip", len(vals))
			continue
		}

		user, database, address := vals[0], vals[1], vals[2]

		ch <- c.clients.mustNewConstMetric(v, user, database, address)
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
	log.Debug("parse pgbouncer pools stats")

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
		poolname := strings.Join([]string{stat.user, stat.database, stat.mode}, "/")

		stats[poolname] = stat

		for i, colname := range r.Colnames {
			// Column's values act as metric values or as labels values.
			// If column's name is NOT in the labelNames, process column's values as values for metrics. If column's name
			// is in the labelNames, skip that column.
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
				log.Errorf("invalid input, parse '%s' failed: %s, skip", row[i].String, err)
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

	return stats
}

// parsePgbouncerClientsStats parses query result and returns connected clients stats.
func parsePgbouncerClientsStats(r *model.PGResult) map[string]float64 {
	log.Debug("parse pgbouncer clients stats")

	var stats = map[string]float64{}

	for _, row := range r.Rows {
		var user, database, address string

		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "user":
				user = row[i].String
			case "database":
				database = row[i].String
			case "addr":
				address = row[i].String
			}
			// skip all other columns
		}

		// create a client consisting of trio user/database/address
		client := strings.Join([]string{user, database, address}, "/")

		stats[client]++
	}

	return stats
}
