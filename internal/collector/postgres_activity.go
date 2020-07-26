package collector

import (
	"context"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"regexp"
	"strconv"
)

const (
	postgresActivityQuery = `SELECT
    state, wait_event_type, wait_event,
    coalesce(extract(epoch FROM clock_timestamp() - coalesce(xact_start, query_start))) AS since_start_seconds,
    coalesce(extract(epoch FROM clock_timestamp() - state_change)) AS since_change_seconds,
    left(query, 32) as query
FROM pg_stat_activity
`
	postgresPreparedXactQuery = `SELECT count(*) AS total FROM pg_prepared_xacts`
)

/*
   *** IMPORTANT: основная сложность в том что активность определяется по нескольким источникам, например по полям state,
   wait_event_type и вообще на основе двух представлений. Может получиться так что бэкенд учитывается в двух местах, например
   в active и waiting. Кроме того есть еще backend_type != 'client_backend', который вносит некоторую путаницу при учете через
   state/wait_event_type. Поэтому нельзя так просто взять и сложить все типы и получить total - полученное значение будет
   больше чем реальный total. Именно поэтому есть отдельно посчитанный total
*/

// postgresActivityStat describes current activity
type postgresActivityStat struct {
	total    float64 // state IS NOT NULL
	idle     float64 // state = 'idle'
	idlexact float64 // state IN ('idle in transaction', 'idle in transaction (aborted)'))
	active   float64 // state = 'active'
	other    float64 // state IN ('fastpath function call','disabled')
	waiting  float64 // wait_event_type = 'Lock'
	prepared float64 // FROM pg_prepared_xacts
	maxuser  float64 // longest duration among client queries
	maxmaint float64 // longest duration among maintenance operations (autovacuum, vacuum. analyze)
}

func (s *postgresActivityStat) updateState(state string) {
	// increment state-specific counter
	switch state {
	case "active":
		s.total++
		s.active++
	case "idle":
		s.total++
		s.idle++
	case "idle in transaction", "idle in transaction (aborted)":
		s.total++
		s.idlexact++
	case "fastpath function call", "disabled":
		s.total++
		s.other++
	case "waiting":
		// waiting must not increment totals because it isn't based on state column
		s.waiting++
	}
}

func (s *postgresActivityStat) updateMaxDuration(value string, state string, query string) {
	if value == "" || state == "" || query == "" {
		return
	}

	// don't account time for idle connections
	if state == "idle" {
		return
	}

	v, err := strconv.ParseFloat(value, 64)
	if err != nil {
		log.Errorf("skip collecting max duration metric: %s", err)
		return
	}

	// all validations ok, update stats

	re, err := regexp.Compile(`^(?i)(autovacuum:|vacuum|analyze)`)
	if err != nil {
		log.Errorf("skip collecting max duration metric: %s", err)
		return
	}

	if re.MatchString(query) {
		if v > s.maxmaint {
			s.maxmaint = v
		}
	} else {
		if v > s.maxuser {
			s.maxuser = v
		}
	}
}

// postgresActivityCollector ...
type postgresActivityCollector struct {
	descs map[string]typedDesc
	postgresActivityStat
}

// NewPostgresActivityCollector returns a new Collector exposing postgres databases stats.
// For details see
// 1. https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ACTIVITY-VIEW
// 2. https://www.postgresql.org/docs/current/view-pg-prepared-xacts.html
func NewPostgresActivityCollector(constLabels prometheus.Labels) (Collector, error) {
	return &postgresActivityCollector{
		descs: map[string]typedDesc{
			"conn_state": {
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "activity", "conn_total"),
					"The total number of connections in each state.",
					[]string{"state"}, constLabels,
				), valueType: prometheus.GaugeValue,
			},
			"activity_max_seconds": {
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "activity", "max_seconds"),
					"The current longest activity for each type of activity.",
					[]string{"type"}, constLabels,
				), valueType: prometheus.GaugeValue,
			},
			"prepared_xact": {
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "activity", "prepared_xact_total"),
					"The total number of transactions that are currently prepared for two-phase commit.",
					nil, constLabels,
				), valueType: prometheus.GaugeValue,
			},
		},
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresActivityCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.NewDB(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	// get pg_stat_activity stats
	res, err := conn.GetStats(postgresActivityQuery)
	if err != nil {
		return err
	}

	// parse pg_stat_activity stats
	stats := parsePostgresActivityStats(res)

	// get pg_prepared_xacts stats
	var count int
	err = conn.Conn.QueryRow(context.Background(), postgresPreparedXactQuery).Scan(&count)
	if err != nil {
		log.Warnf("failed to read pg_prepared_xacts: %s; skip", err)
		delete(c.descs, "prepared_xact")
	} else {
		stats.prepared = float64(count)
	}

	for name, desc := range c.descs {
		switch name {
		case "conn_state":
			ch <- desc.mustNewConstMetric(stats.total, "total")
			ch <- desc.mustNewConstMetric(stats.active, "active")
			ch <- desc.mustNewConstMetric(stats.idle, "idle")
			ch <- desc.mustNewConstMetric(stats.idlexact, "idlexact")
			ch <- desc.mustNewConstMetric(stats.other, "other")
			ch <- desc.mustNewConstMetric(stats.waiting, "waiting")
		case "prepared_xact":
			ch <- desc.mustNewConstMetric(stats.prepared)
		case "activity_max_seconds":
			ch <- desc.mustNewConstMetric(stats.maxuser, "user")
			ch <- desc.mustNewConstMetric(stats.maxmaint, "maintenance")
		default:
			log.Debugf("unknown desc name: %s, skip", name)
			continue
		}
	}

	return nil
}

func parsePostgresActivityStats(r *store.QueryResult) postgresActivityStat {
	var stats postgresActivityStat

	// Make map with column names and their indexes. This map needed to get quick access to values of exact columns within
	// processed row.
	var colindexes = map[string]int{}
	for i, colname := range r.Colnames {
		colindexes[string(colname.Name)] = i
	}

	for _, row := range r.Rows {
		for i, colname := range r.Colnames {
			// Skip empty (NULL) values.
			if row[i].String == "" {
				log.Debug("got empty (NULL) value, skip")
				continue
			}

			// Run column-specific logic
			switch string(colname.Name) {
			case "state":
				// Count activity only if query is not NULL (if query is NULL it means this is a background server process
				// and is not a client backend).
				idx := colindexes["query"]
				if row[idx].String != "" && row[idx].Valid {
					stats.updateState(row[i].String)
				}
			case "wait_event_type":
				if row[i].String == "Lock" {
					stats.updateState("waiting")
				}
			case "since_start_seconds":
				stateIdx := colindexes["state"]
				queryIdx := colindexes["query"]
				value := row[i].String

				if row[queryIdx].Valid && row[stateIdx].Valid {
					state := row[stateIdx].String
					query := row[queryIdx].String
					stats.updateMaxDuration(value, state, query)
				}
			default:
				log.Debugf("unsupported pg_stat_activity stat column: %s, skip", string(colname.Name))
				continue
			}

			// Get data value and convert it to float64 used by Prometheus.
			//v, err := strconv.ParseFloat(row[i].String, 64)
			//if err != nil {
			//  log.Errorf("skip collecting metric: %s", err)
			//  continue
			//}
		}
	}

	return stats
}
