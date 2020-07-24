package collector

import (
	"database/sql"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"strings"
)

const (
	// names of columns used in "SHOW POOLS" output. For details see https://www.pgbouncer.org/usage.html#show-pools
	cnameDatabase  = "database"
	cnameUser      = "user"
	cnameClActive  = "cl_active"
	cnameClWaiting = "cl_waiting"
	cnameSvActive  = "sv_active"
	cnameSvIdle    = "sv_idle"
	cnameSvUsed    = "sv_used"
	cnameSvTested  = "sv_tested"
	cnameSvLogin   = "sv_login"
	cnameMaxwait   = "maxwait"
	cnamePoolMode  = "pool_mode"

	// admin console query used for retrieving pools stats
	poolQuery = "SHOW POOLS"
)

type pgbouncerPoolsCollector struct {
	descs      []typedDesc
	labelNames []string
	connStats  map[string]connStat
	conns      *prometheus.Desc
	maxwait    *prometheus.Desc
}

type connStat struct {
	clActive  float64
	clWaiting float64
	svActive  float64
	svIdle    float64
	svUsed    float64
	svTested  float64
	svLogin   float64
	maxWait   float64
}

// NewPgbouncerPoolsCollector returns a new Collector exposing pgbouncer pools connections usage stats.
// For details see https://www.pgbouncer.org/usage.html#show-pools.
func NewPgbouncerPoolsCollector(constLabels prometheus.Labels) (Collector, error) {
	var poolsLabelNames = []string{cnameDatabase, cnameUser, cnamePoolMode, "state"}

	return &pgbouncerPoolsCollector{
		conns: prometheus.NewDesc(
			prometheus.BuildFQName("pgscv", "pgbouncer", "pool_conn_total"),
			"The total number of connections established.",
			poolsLabelNames, constLabels,
		),
		maxwait: prometheus.NewDesc(
			prometheus.BuildFQName("pgscv", "pgbouncer", "pool_max_wait_seconds"),
			"Total time the first (oldest) client in the queue has waited, in seconds.",
			[]string{cnameDatabase, cnameUser, cnamePoolMode}, constLabels,
		),
		labelNames: poolsLabelNames,
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *pgbouncerPoolsCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.NewDB(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	res, err := conn.GetStats(poolQuery)
	if err != nil {
		return err
	}

	stats := parsePgbouncerPoolsStats(res, c.labelNames)

	for poolname, poolstat := range stats {
		props := strings.Split(poolname, "/")
		if len(props) != 3 {
			log.Warnf("incomplete pool name: %s; skip", poolname)
			continue
		}
		ch <- prometheus.MustNewConstMetric(c.conns, prometheus.GaugeValue, poolstat.clActive, props[0], props[1], props[2], cnameClActive)
		ch <- prometheus.MustNewConstMetric(c.conns, prometheus.GaugeValue, poolstat.clWaiting, props[0], props[1], props[2], cnameClWaiting)
		ch <- prometheus.MustNewConstMetric(c.conns, prometheus.GaugeValue, poolstat.svActive, props[0], props[1], props[2], cnameSvActive)
		ch <- prometheus.MustNewConstMetric(c.conns, prometheus.GaugeValue, poolstat.svIdle, props[0], props[1], props[2], cnameSvIdle)
		ch <- prometheus.MustNewConstMetric(c.conns, prometheus.GaugeValue, poolstat.svUsed, props[0], props[1], props[2], cnameSvUsed)
		ch <- prometheus.MustNewConstMetric(c.conns, prometheus.GaugeValue, poolstat.svTested, props[0], props[1], props[2], cnameSvTested)
		ch <- prometheus.MustNewConstMetric(c.conns, prometheus.GaugeValue, poolstat.svLogin, props[0], props[1], props[2], cnameSvLogin)
		ch <- prometheus.MustNewConstMetric(c.maxwait, prometheus.GaugeValue, poolstat.maxWait, props[0], props[1], props[2])
	}

	return nil
}

func parsePgbouncerPoolsStats(r *store.QueryResult, labelNames []string) map[string]connStat {
	// ad-hoc struct used to group pool properties (database, user and mode) in one place.
	type poolProperties struct {
		database string
		user     string
		mode     string
	}

	var stats = map[string]connStat{}
	var poolname string

	for _, row := range r.Rows {
		props := poolProperties{}
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case cnameDatabase:
				props.database = row[i].String
			case cnameUser:
				props.user = row[i].String
			case cnamePoolMode:
				props.mode = row[i].String
			}
		}

		// create a pool name consisting of trio database/user/pool_mode
		poolname = strings.Join([]string{props.database, props.user, props.mode}, "/")

		for i, colname := range r.Colnames {
			// Column's values act as metric values or as labels values.
			// If column's name is NOT in the labelNames, process column's values as values for metrics. If column's name
			// is in the labelNames, skip that column.
			if !stringsContains(labelNames, string(colname.Name)) {
				// Empty (NULL) values are converted to zeros.
				if row[i].String == "" {
					log.Debug("got empty value, convert it to zero")
					row[i] = sql.NullString{String: "0", Valid: true}
				}

				// Get data value and convert it to float64 used by Prometheus.
				v, err := strconv.ParseFloat(row[i].String, 64)
				if err != nil {
					log.Errorf("skip collecting metric: %s", err)
					continue
				}

				// Update stats struct
				switch string(colname.Name) {
				case cnameClActive:
					s := stats[poolname]
					s.clActive = v
					stats[poolname] = s
				case cnameClWaiting:
					s := stats[poolname]
					s.clWaiting = v
					stats[poolname] = s
				case cnameSvActive:
					s := stats[poolname]
					s.svActive = v
					stats[poolname] = s
				case cnameSvIdle:
					s := stats[poolname]
					s.svIdle = v
					stats[poolname] = s
				case cnameSvUsed:
					s := stats[poolname]
					s.svUsed = v
					stats[poolname] = s
				case cnameSvTested:
					s := stats[poolname]
					s.svTested = v
					stats[poolname] = s
				case cnameSvLogin:
					s := stats[poolname]
					s.svLogin = v
					stats[poolname] = s
				case cnameMaxwait:
					s := stats[poolname]
					s.maxWait = v
					stats[poolname] = s
				default:
					log.Debugf("unsupported pool stat column: %s, skip", string(colname.Name))
					continue
				}
			}
		}
	}

	return stats
}
