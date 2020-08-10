package collector

import (
	"context"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"regexp"
	"strconv"
)

const (
	postgresActivityQuery95 = `SELECT
    state, waiting,
    coalesce(extract(epoch FROM clock_timestamp() - coalesce(xact_start, query_start))) AS since_start_seconds,
    coalesce(extract(epoch FROM clock_timestamp() - state_change)) AS since_change_seconds,
    left(query, 32) as query
FROM pg_stat_activity`

	postgresActivityQueryLatest = `SELECT
    state, wait_event_type, wait_event,
    coalesce(extract(epoch FROM clock_timestamp() - coalesce(xact_start, query_start))) AS since_start_seconds,
    coalesce(extract(epoch FROM clock_timestamp() - state_change)) AS since_change_seconds,
    left(query, 32) as query
FROM pg_stat_activity`

	postgresPreparedXactQuery = `SELECT count(*) AS total FROM pg_prepared_xacts`

	// Backend states accordingly to pg_stat_activity.state
	stActive          = "active"
	stIdle            = "idle"
	stIdleXact        = "idle in transaction"
	stIdleXactAborted = "idle in transaction (aborted)"
	stFastpath        = "fastpath function call"
	stDisabled        = "disabled"
	stWaiting         = "waiting" // fake state based on 'wait_event_type'

	// Wait event type names
	weLock = "Lock"

	// regexpMaintenanceActivity defines what queries should be considered as maintenance operations.
	regexpMaintenanceActivity = `^(?i)(autovacuum:|vacuum|analyze)`
)

// postgresActivityCollector ...
type postgresActivityCollector struct {
	states   typedDesc
	activity typedDesc
	prepared typedDesc
	inflight typedDesc
}

// NewPostgresActivityCollector returns a new Collector exposing postgres databases stats.
// For details see
// 1. https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ACTIVITY-VIEW
// 2. https://www.postgresql.org/docs/current/view-pg-prepared-xacts.html
func NewPostgresActivityCollector(constLabels prometheus.Labels) (Collector, error) {
	return &postgresActivityCollector{
		states: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "activity", "conn_total"),
				"The total number of connections in each state.",
				[]string{"state"}, constLabels,
			), valueType: prometheus.GaugeValue,
		},
		activity: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "activity", "max_seconds"),
				"The current longest activity for each type of activity.",
				[]string{"state", "type"}, constLabels,
			), valueType: prometheus.GaugeValue,
		},
		prepared: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "activity", "prepared_xact_total"),
				"The total number of transactions that are currently prepared for two-phase commit.",
				nil, constLabels,
			), valueType: prometheus.GaugeValue,
		},
		inflight: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "activity", "queries_in_flight"),
				"The total number of queries executed in-flight of each type.",
				[]string{"type"}, constLabels,
			), valueType: prometheus.GaugeValue,
		},
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresActivityCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	// get pg_stat_activity stats
	res, err := conn.Query(selectActivityQuery(config.ServerVersionNum))
	if err != nil {
		return err
	}

	// parse pg_stat_activity stats
	stats := parsePostgresActivityStats(res)

	// get pg_prepared_xacts stats
	var count int
	err = conn.Conn().QueryRow(context.Background(), postgresPreparedXactQuery).Scan(&count)
	if err != nil {
		log.Warnf("failed to read pg_prepared_xacts: %s; skip", err)
	} else {
		stats.prepared = float64(count)
	}

	// connection states
	// totals doesn't account waitings because they have 'active' state.
	var total = stats.active + stats.idle + stats.idlexact + stats.other
	ch <- c.states.mustNewConstMetric(total, "total")
	ch <- c.states.mustNewConstMetric(stats.active, "active")
	ch <- c.states.mustNewConstMetric(stats.idle, "idle")
	ch <- c.states.mustNewConstMetric(stats.idlexact, "idlexact")
	ch <- c.states.mustNewConstMetric(stats.other, "other")
	ch <- c.states.mustNewConstMetric(stats.waiting, "waiting")

	// prepared xacts
	ch <- c.prepared.mustNewConstMetric(stats.prepared)

	// activity max times
	ch <- c.activity.mustNewConstMetric(stats.maxIdleUser, "idle_xact", "user")
	ch <- c.activity.mustNewConstMetric(stats.maxIdleMaint, "idle_xact", "maintenance")
	ch <- c.activity.mustNewConstMetric(stats.maxRunUser, "running", "user")
	ch <- c.activity.mustNewConstMetric(stats.maxRunMaint, "running", "maintenance")
	ch <- c.activity.mustNewConstMetric(stats.maxWaitUser, "waiting", "user")
	ch <- c.activity.mustNewConstMetric(stats.maxWaitMaint, "waiting", "maintenance")

	// in flight queries
	ch <- c.inflight.mustNewConstMetric(stats.querySelect, "select")
	ch <- c.inflight.mustNewConstMetric(stats.queryMod, "mod")
	ch <- c.inflight.mustNewConstMetric(stats.queryDdl, "ddl")
	ch <- c.inflight.mustNewConstMetric(stats.queryMaint, "maintenance")
	ch <- c.inflight.mustNewConstMetric(stats.queryWith, "with")
	ch <- c.inflight.mustNewConstMetric(stats.queryCopy, "copy")
	ch <- c.inflight.mustNewConstMetric(stats.queryOther, "other")

	return nil
}

/*
   *** IMPORTANT: основная сложность в том что активность определяется по нескольким источникам, например по полям state,
   wait_event_type и вообще на основе двух представлений. Может получиться так что бэкенд учитывается в двух местах, например
   в active и waiting. Кроме того есть еще backend_type != 'client_backend', который вносит некоторую путаницу при учете через
   state/wait_event_type. Поэтому нельзя так просто взять и сложить все типы и получить total - полученное значение будет
   больше чем реальный total. Именно поэтому есть отдельно посчитанный total.
*/

// postgresActivityStat describes current activity
type postgresActivityStat struct {
	idle         float64 // state = 'idle'
	idlexact     float64 // state IN ('idle in transaction', 'idle in transaction (aborted)'))
	active       float64 // state = 'active'
	other        float64 // state IN ('fastpath function call','disabled')
	waiting      float64 // wait_event_type = 'Lock' (or waiting = 't')
	prepared     float64 // FROM pg_prepared_xacts
	maxIdleUser  float64 // longest duration among idle transactions opened by user
	maxIdleMaint float64 // longest duration among idle transactions initiated by maintenance operations (autovacuum, vacuum. analyze)
	maxRunUser   float64 // longest duration among client queries
	maxRunMaint  float64 // longest duration among maintenance operations (autovacuum, vacuum. analyze)
	maxWaitUser  float64 // longest duration being in waiting state (all activity)
	maxWaitMaint float64 // longest duration being in waiting state (all activity)
	querySelect  float64 // number of select queries: SELECT, TABLE
	queryMod     float64 // number of DML: INSERT, UPDATE, DELETE, TRUNCATE
	queryDdl     float64 // number of DDL queries: CREATE, ALTER, DROP
	queryMaint   float64 // number of maintenance queries: VACUUM, ANALYZE, CLUSTER, REINDEX, REFRESH, CHECKPOINT
	queryWith    float64 // number of CTE queries
	queryCopy    float64 // number of COPY queries
	queryOther   float64 // number of queries of other types: BEGIN, END, COMMIT, ABORT, SET, etc...
}

func parsePostgresActivityStats(r *model.PGResult) postgresActivityStat {
	var stats postgresActivityStat

	// Depending on Postgres version, waiting backends are observed using different column: 'waiting' used in 9.5 and older
	// and 'wait_event_type' from 9.6. waitColumnName defines a name of column which will be used for detecting waitings.
	// By default use "wait_event_type"
	var waitColumnName = "wait_event_type"

	// Make map with column names and their indexes. This map needed to get quick access to values of exact columns within
	// processed row.
	var colindexes = map[string]int{}
	for i, colname := range r.Colnames {
		colindexes[string(colname.Name)] = i

		if string(colname.Name) == "waiting" {
			waitColumnName = "waiting"
		}
	}

	for _, row := range r.Rows {
		for i, colname := range r.Colnames {
			// Skip empty (NULL) values.
			if row[i].String == "" {
				log.Debug("got empty (NULL) value, skip")
				continue
			}

			// Run column-specific logic. All empty (NULL) or not Valid values are silently ignored.
			switch string(colname.Name) {
			case "state":
				// Count activity only if query is not NULL (if query is NULL it means this is a background server process
				// and is not a client backend).
				idx := colindexes["query"]
				if row[idx].String != "" && row[idx].Valid {
					stats.updateState(row[i].String)
				}
			case waitColumnName:
				// Count waitings only if waiting = 't' or wait_event_type = 'Lock'.
				if row[i].String == weLock || row[i].String == "t" {
					stats.updateState("waiting")
				}
			case "since_start_seconds":
				// Consider type of activity depending on 'state' column.
				stateIdx := colindexes["state"]
				eventIdx := colindexes[waitColumnName]
				queryIdx := colindexes["query"]

				if row[stateIdx].Valid && row[queryIdx].Valid {
					value := row[i].String
					state := row[stateIdx].String
					event := row[eventIdx].String
					query := row[queryIdx].String
					if state == stIdleXact || state == stIdleXactAborted {
						stats.updateMaxIdletimeDuration(value, state, query)
					} else {
						stats.updateMaxRuntimeDuration(value, state, event, query)
					}
				}
			case "since_change_seconds":
				eventIdx := colindexes[waitColumnName]
				queryIdx := colindexes["query"]

				if row[eventIdx].Valid && row[queryIdx].Valid {
					value := row[i].String
					event := row[eventIdx].String
					query := row[queryIdx].String
					stats.updateMaxWaittimeDuration(value, event, query)
				}
			case "query":
				stateIdx := colindexes["state"]

				if row[stateIdx].Valid {
					value := row[i].String
					state := row[stateIdx].String
					stats.updateQueryStat(value, state)
				}
			default:
				log.Debugf("unsupported pg_stat_activity stat column: %s, skip", string(colname.Name))
				continue
			}
		}
	}

	return stats
}

// updateState increments counter depending on passed state of the backend.
func (s *postgresActivityStat) updateState(state string) {
	// increment state-specific counter
	switch state {
	case stActive:
		s.active++
	case stIdle:
		s.idle++
	case stIdleXact, stIdleXactAborted:
		s.idlexact++
	case stFastpath, stDisabled:
		s.other++
	case stWaiting:
		// waiting must not increment totals because it isn't based on state column
		s.waiting++
	}
}

// updateMaxIdletimeDuration updates max duration of idle transactions activity.
func (s *postgresActivityStat) updateMaxIdletimeDuration(value string, state string, query string) {
	// necessary values should not be empty (except wait_event_type)
	if value == "" || state == "" || query == "" {
		return
	}

	// don't account time for any activity except idle xacts.
	if state != stIdleXact && state != stIdleXactAborted {
		return
	}

	v, err := strconv.ParseFloat(value, 64)
	if err != nil {
		log.Errorf("skip collecting max idle time duration metric: %s", err)
		return
	}

	// all validations ok, update stats

	// inspect query - is ia a user activity like queries, or maintenance tasks like automatic or regular vacuum/analyze.
	re, err := regexp.Compile(regexpMaintenanceActivity)
	if err != nil {
		log.Errorf("skip collecting max idle time duration metric: %s", err)
		return
	}

	if re.MatchString(query) {
		if v > s.maxIdleMaint {
			s.maxIdleMaint = v
		}
	} else {
		if v > s.maxIdleUser {
			s.maxIdleUser = v
		}
	}
}

// updateMaxRuntimeDuration updates max duration o frunning activity.
func (s *postgresActivityStat) updateMaxRuntimeDuration(value string, state string, etype string, query string) {
	// necessary values should not be empty (except wait_event_type)
	if value == "" || state == "" || query == "" {
		return
	}

	// don't account time for idle or blocked connections, interesting only for running activity.
	if state == stIdle || etype == weLock {
		return
	}

	v, err := strconv.ParseFloat(value, 64)
	if err != nil {
		log.Errorf("skip collecting max run time duration metric: %s", err)
		return
	}

	// all validations ok, update stats

	// inspect query - is ia a user activity like queries, or maintenance tasks like automatic or regular vacuum/analyze.
	re, err := regexp.Compile(regexpMaintenanceActivity)
	if err != nil {
		log.Errorf("skip collecting max run time duration metric: %s", err)
		return
	}

	if re.MatchString(query) {
		if v > s.maxRunMaint {
			s.maxRunMaint = v
		}
	} else {
		if v > s.maxRunUser {
			s.maxRunUser = v
		}
	}
}

// updateMaxWaittimeDuration updates max duration of waiting activity.
func (s *postgresActivityStat) updateMaxWaittimeDuration(value string, waiting string, query string) {
	if value == "" || waiting == "" || query == "" {
		return
	}

	// waiting activity is considered only with wait_event_type = 'Lock' (or waiting = 't')
	if waiting != weLock && waiting != "t" {
		return
	}

	v, err := strconv.ParseFloat(value, 64)
	if err != nil {
		log.Errorf("skip collecting max wait time duration metric: %s", err)
		return
	}

	// all validations ok, update stats
	re, err := regexp.Compile(regexpMaintenanceActivity)
	if err != nil {
		log.Errorf("skip collecting max wait time duration metric: %s", err)
		return
	}

	if re.MatchString(query) {
		if v > s.maxWaitMaint {
			s.maxWaitMaint = v
		}
	} else {
		if v > s.maxWaitUser {
			s.maxWaitUser = v
		}
	}
}

func (s *postgresActivityStat) updateQueryStat(query string, state string) {
	if state != stActive {
		return
	}

	var pattern = `^(?i)(SELECT|TABLE)`
	re, err := regexp.Compile(pattern)
	if err != nil {
		log.Errorf("failed compile regex pattern %s: %s", pattern, err)
	}
	if re.MatchString(query) {
		s.querySelect++
		return
	}

	pattern = `^(?i)(INSERT|UPDATE|DELETE|TRUNCATE)`
	re, err = regexp.Compile(pattern)
	if err != nil {
		log.Errorf("failed compile regex pattern %s: %s", pattern, err)
	}
	if re.MatchString(query) {
		s.queryMod++
		return
	}

	pattern = `^(?i)(CREATE|ALTER|DROP)`
	re, err = regexp.Compile(pattern)
	if err != nil {
		log.Errorf("failed compile regex pattern %s: %s", pattern, err)
	}
	if re.MatchString(query) {
		s.queryDdl++
		return
	}

	pattern = `^(?i)(VACUUM|ANALYZE|CLUSTER|REINDEX|REFRESH|CHECKPOINT|autovacuum:)`
	re, err = regexp.Compile(pattern)
	if err != nil {
		log.Errorf("failed compile regex pattern %s: %s", pattern, err)
	}
	if re.MatchString(query) {
		s.queryMaint++
		return
	}

	pattern = `^(?i)WITH`
	re, err = regexp.Compile(pattern)
	if err != nil {
		log.Errorf("failed compile regex pattern %s: %s", pattern, err)
	}
	if re.MatchString(query) {
		s.queryWith++
		return
	}

	pattern = `^(?i)COPY`
	re, err = regexp.Compile(pattern)
	if err != nil {
		log.Errorf("failed compile regex pattern %s: %s", pattern, err)
	}
	if re.MatchString(query) {
		s.queryCopy++
		return
	}

	// still here? ok, increment others and return
	s.queryOther++
}

// selectActivityQuery returns suitable activity query depending on passed version.
func selectActivityQuery(version int) string {
	switch {
	case version < PostgresV96:
		return postgresActivityQuery95
	default:
		return postgresActivityQueryLatest
	}
}
