package collector

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"regexp"
	"strconv"
	"strings"
)

const (
	postgresActivityQuery95 = "SELECT " +
		"coalesce(usename, 'system') AS usename, coalesce(datname, 'none') AS datname, state, waiting, " +
		"extract(epoch FROM clock_timestamp() - coalesce(xact_start, query_start)) AS since_start_seconds, " +
		"extract(epoch FROM clock_timestamp() - state_change) AS since_change_seconds, " +
		"left(query, 32) as query " +
		"FROM pg_stat_activity"

	postgresActivityQueryLatest = "SELECT " +
		"coalesce(usename, backend_type) AS usename, coalesce(datname, 'none') AS datname, state, wait_event_type, wait_event, " +
		"extract(epoch FROM clock_timestamp() - coalesce(xact_start, query_start)) AS since_start_seconds, " +
		"extract(epoch FROM clock_timestamp() - state_change) AS since_change_seconds, " +
		"left(query, 32) as query " +
		"FROM pg_stat_activity"

	postgresPreparedXactQuery = "SELECT count(*) AS total FROM pg_prepared_xacts"

	// Backend states accordingly to pg_stat_activity.state
	stActive          = "active"
	stIdle            = "idle"
	stIdleXact        = "idle in transaction"
	stIdleXactAborted = "idle in transaction (aborted)"
	stFastpath        = "fastpath function call"
	stDisabled        = "disabled"
	stWaiting         = "waiting" // fake state based on 'wait_event_type == Lock'

	// Wait event type names
	weLock = "Lock"
)

// postgresActivityCollector ...
type postgresActivityCollector struct {
	waitEvents typedDesc
	states     typedDesc
	statesAll  typedDesc
	activity   typedDesc
	prepared   typedDesc
	inflight   typedDesc
	vacuums    typedDesc
	re         queryRegexp // regexps for queries classification
}

// NewPostgresActivityCollector returns a new Collector exposing postgres databases stats.
// For details see
// 1. https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ACTIVITY-VIEW
// 2. https://www.postgresql.org/docs/current/view-pg-prepared-xacts.html
func NewPostgresActivityCollector(constLabels prometheus.Labels, _ model.CollectorSettings) (Collector, error) {
	return &postgresActivityCollector{
		waitEvents: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "activity", "wait_events_in_flight"),
				"Number of wait events in-flight in each state.",
				[]string{"type", "event"}, constLabels,
			), valueType: prometheus.GaugeValue,
		},
		states: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "activity", "connections_in_flight"),
				"Number of connections in-flight in each state.",
				[]string{"state"}, constLabels,
			), valueType: prometheus.GaugeValue,
		},
		statesAll: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "activity", "connections_all_in_flight"),
				"Number of all connections in-flight.",
				nil, constLabels,
			), valueType: prometheus.GaugeValue,
		},
		activity: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "activity", "max_seconds"),
				"Longest activity for each user, database and activity type.",
				[]string{"usename", "datname", "state", "type"}, constLabels,
			), valueType: prometheus.GaugeValue,
		},
		prepared: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "activity", "prepared_transactions_in_flight"),
				"Number of transactions that are currently prepared for two-phase commit.",
				nil, constLabels,
			), valueType: prometheus.GaugeValue,
		},
		inflight: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "activity", "queries_in_flight"),
				"Number of queries running in-flight of each type.",
				[]string{"type"}, constLabels,
			), valueType: prometheus.GaugeValue,
		},
		vacuums: typedDesc{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName("postgres", "activity", "vacuums_in_flight"),
				"Number of vacuum operations running in-flight of each type.",
				[]string{"type"}, constLabels,
			), valueType: prometheus.GaugeValue,
		},
		re: newQueryRegexp(),
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
	stats := parsePostgresActivityStats(res, c.re)

	// get pg_prepared_xacts stats
	var count int
	err = conn.Conn().QueryRow(context.Background(), postgresPreparedXactQuery).Scan(&count)
	if err != nil {
		log.Warnf("query pg_prepared_xacts failed: %s; skip", err)
	} else {
		stats.prepared = float64(count)
	}

	// Send collected metrics.

	// wait_events
	for k, v := range stats.waitEvents {
		// 'key' is the pair of wait_event_type/wait_event - split them and use as label values.
		if labels := strings.Split(k, "/"); len(labels) >= 2 {
			ch <- c.waitEvents.newConstMetric(v, labels[0], labels[1])
		} else {
			log.Warnf("create wait_event activity failed: invalid input '%s'; skip", k)
		}
	}

	// connection states
	// totals doesn't account waitings because they have 'active' state.
	var total = stats.active + stats.idle + stats.idlexact + stats.other
	ch <- c.statesAll.newConstMetric(total)
	ch <- c.states.newConstMetric(stats.active, "active")
	ch <- c.states.newConstMetric(stats.idle, "idle")
	ch <- c.states.newConstMetric(stats.idlexact, "idlexact")
	ch <- c.states.newConstMetric(stats.other, "other")
	ch <- c.states.newConstMetric(stats.waiting, "waiting")

	// prepared xacts
	ch <- c.prepared.newConstMetric(stats.prepared)

	// max duration of user's idle_xacts per usename/datname.
	for k, v := range stats.maxIdleUser {
		if names := strings.Split(k, "/"); len(names) >= 2 {
			ch <- c.activity.newConstMetric(v, names[0], names[1], "idlexact", "user")
		} else {
			log.Warnf("create max idlexact user activity failed: invalid input '%s'; skip", k)
		}
	}

	// max duration of maintenance's idle_xacts per usename/datname.
	for k, v := range stats.maxIdleMaint {
		if names := strings.Split(k, "/"); len(names) >= 2 {
			ch <- c.activity.newConstMetric(v, names[0], names[1], "idlexact", "maintenance")
		} else {
			log.Warnf("create max idlexact maintenance activity failed: invalid input '%s'; skip", k)
		}
	}

	// max duration of users running activity per usename/datname.
	for k, v := range stats.maxRunUser {
		if names := strings.Split(k, "/"); len(names) >= 2 {
			ch <- c.activity.newConstMetric(v, names[0], names[1], "running", "user")
		} else {
			log.Warnf("create max running user activity failed: invalid input '%s'; skip", k)
		}
	}

	// max duration of maintenance running activity per usename/datname.
	for k, v := range stats.maxRunMaint {
		if names := strings.Split(k, "/"); len(names) >= 2 {
			ch <- c.activity.newConstMetric(v, names[0], names[1], "running", "maintenance")
		} else {
			log.Warnf("create max running maintenance activity failed: invalid input '%s'; skip", k)
		}
	}

	// max duration of users waiting activity per usename/datname.
	for k, v := range stats.maxWaitUser {
		if names := strings.Split(k, "/"); len(names) >= 2 {
			ch <- c.activity.newConstMetric(v, names[0], names[1], "waiting", "user")
		} else {
			log.Warnf("create max waiting user activity failed: invalid input '%s'; skip", k)
		}
	}

	// max duration of maintenance waiting activity per usename/datname.
	for k, v := range stats.maxWaitMaint {
		if names := strings.Split(k, "/"); len(names) >= 2 {
			ch <- c.activity.newConstMetric(v, names[0], names[1], "waiting", "maintenance")
		} else {
			log.Warnf("create max waiting maintenance activity failed: invalid input '%s'; skip", k)
		}
	}

	// in flight queries
	ch <- c.inflight.newConstMetric(stats.querySelect, "select")
	ch <- c.inflight.newConstMetric(stats.queryMod, "mod")
	ch <- c.inflight.newConstMetric(stats.queryDdl, "ddl")
	ch <- c.inflight.newConstMetric(stats.queryMaint, "maintenance")
	ch <- c.inflight.newConstMetric(stats.queryWith, "with")
	ch <- c.inflight.newConstMetric(stats.queryCopy, "copy")
	ch <- c.inflight.newConstMetric(stats.queryOther, "other")

	// vacuums
	for k, v := range stats.vacuumOps {
		ch <- c.vacuums.newConstMetric(v, k)
	}

	return nil
}

// queryRegexp used for keeping regexps for query classification.
// It's created (compiled) at startup and used during program lifetime.
type queryRegexp struct {
	// query regexps
	selects *regexp.Regexp // SELECT|TABLE
	mod     *regexp.Regexp // INSERT|UPDATE|DELETE|TRUNCATE
	ddl     *regexp.Regexp // CREATE|ALTER|DROP
	maint   *regexp.Regexp // ANALYZE|CLUSTER|REINDEX|REFRESH|CHECKPOINT
	vacuum  *regexp.Regexp // VACUUM|autovacuum: .+
	vacanl  *regexp.Regexp // VACUUM|ANALYZE|autovacuum:
	with    *regexp.Regexp // WITH
	copy    *regexp.Regexp // COPY
}

// newQueryRegexp creates new queryRegexp with compiled regexp objects.
func newQueryRegexp() queryRegexp {
	return queryRegexp{
		// compile regexp objects
		selects: regexp.MustCompile(`^(?i)(SELECT|TABLE)`),
		mod:     regexp.MustCompile(`^(?i)(INSERT|UPDATE|DELETE|TRUNCATE)`),
		ddl:     regexp.MustCompile(`^(?i)(CREATE|ALTER|DROP)`),
		maint:   regexp.MustCompile(`^(?i)(ANALYZE|CLUSTER|REINDEX|REFRESH|CHECKPOINT)`),
		vacuum:  regexp.MustCompile(`^(?i)(VACUUM|autovacuum: .+)`),
		vacanl:  regexp.MustCompile(`^(?i)(VACUUM|ANALYZE|autovacuum:)`),
		with:    regexp.MustCompile(`^(?i)WITH`),
		copy:    regexp.MustCompile(`^(?i)COPY`),
	}
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
	idle         float64            // state = 'idle'
	idlexact     float64            // state IN ('idle in transaction', 'idle in transaction (aborted)'))
	active       float64            // state = 'active'
	other        float64            // state IN ('fastpath function call','disabled')
	waiting      float64            // wait_event_type = 'Lock' (or waiting = 't')
	waitEvents   map[string]float64 // wait_event_type/wait_event counters
	prepared     float64            // FROM pg_prepared_xacts
	maxIdleUser  map[string]float64 // longest duration among idle transactions opened by user/database
	maxIdleMaint map[string]float64 // longest duration among idle transactions initiated by maintenance operations (autovacuum, vacuum. analyze)
	maxRunUser   map[string]float64 // longest duration among client queries
	maxRunMaint  map[string]float64 // longest duration among maintenance operations (autovacuum, vacuum. analyze)
	maxWaitUser  map[string]float64 // longest duration being in waiting state (all activity)
	maxWaitMaint map[string]float64 // longest duration being in waiting state (all activity)
	querySelect  float64            // number of select queries: SELECT, TABLE
	queryMod     float64            // number of DML: INSERT, UPDATE, DELETE, TRUNCATE
	queryDdl     float64            // number of DDL queries: CREATE, ALTER, DROP
	queryMaint   float64            // number of maintenance queries: VACUUM, ANALYZE, CLUSTER, REINDEX, REFRESH, CHECKPOINT
	queryWith    float64            // number of CTE queries
	queryCopy    float64            // number of COPY queries
	queryOther   float64            // number of queries of other types: BEGIN, END, COMMIT, ABORT, SET, etc...
	vacuumOps    map[string]float64 // vacuum operations by type

	re queryRegexp // regexps used for query classification, it comes from postgresActivityCollector.
}

// newPostgresActivityStat creates new postgresActivityStat struct with initialized maps.
func newPostgresActivityStat(re queryRegexp) postgresActivityStat {
	return postgresActivityStat{
		waitEvents:   make(map[string]float64),
		maxIdleUser:  make(map[string]float64),
		maxIdleMaint: make(map[string]float64),
		maxRunUser:   make(map[string]float64),
		maxRunMaint:  make(map[string]float64),
		maxWaitUser:  make(map[string]float64),
		maxWaitMaint: make(map[string]float64),
		vacuumOps: map[string]float64{
			"wraparound": 0,
			"regular":    0,
			"user":       0,
		},
		re: re,
	}
}

func parsePostgresActivityStats(r *model.PGResult, re queryRegexp) postgresActivityStat {
	log.Debug("parse postgres activity stats")

	var stats = newPostgresActivityStat(re)

	// Depending on Postgres version, waiting backends are observed using different column: 'waiting' used in 9.5 and older
	// and 'wait_event_type' from 9.6. waitColumnName defines a name of column which will be used for detecting waitings.
	// By default use "wait_event_type".
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
			if !row[i].Valid {
				continue
			}

			// Run column-specific logic. All empty (NULL) or not Valid values are silently ignored.
			switch string(colname.Name) {
			case "state":
				// Count activity only if query is not NULL (if query is NULL it means this is a background server process
				// and is not a client backend).
				// Also check backend is not in waiting state. Waiting backends are accounted separately.
				waitColIdx := colindexes[waitColumnName]
				queryColIdx := colindexes["query"]

				if (row[waitColIdx].String != weLock && row[waitColIdx].String != "t") && (row[queryColIdx].String != "" && row[queryColIdx].Valid) {
					stats.updateState(row[i].String)
				}
			case waitColumnName:
				// Count waitings only if waiting = 't' or wait_event_type = 'Lock'.
				if row[i].String == weLock || row[i].String == "t" {
					stats.updateState("waiting")
				}

				// Update wait_event stats for newer Postgres versions.
				if waitColumnName == "wait_event_type" {
					waitEventColIdx := colindexes["wait_event"]

					key := row[i].String + "/" + row[waitEventColIdx].String
					stats.waitEvents[key]++
				}
			case "since_start_seconds":
				// Consider type of activity depending on 'state' column.
				stateIdx := colindexes["state"]
				eventIdx := colindexes[waitColumnName]
				usenameIdx := colindexes["usename"]
				datnameIdx := colindexes["datname"]
				queryIdx := colindexes["query"]

				if row[stateIdx].Valid && row[queryIdx].Valid {
					value := row[i].String
					usename := row[usenameIdx].String
					datname := row[datnameIdx].String
					state := row[stateIdx].String
					event := row[eventIdx].String
					query := row[queryIdx].String
					if state == stIdleXact || state == stIdleXactAborted {
						stats.updateMaxIdletimeDuration(value, usename, datname, state, query)
					} else {
						stats.updateMaxRuntimeDuration(value, usename, datname, state, event, query)
					}
				}
			case "since_change_seconds":
				eventIdx := colindexes[waitColumnName]
				usenameIdx := colindexes["usename"]
				datnameIdx := colindexes["datname"]
				queryIdx := colindexes["query"]

				if row[eventIdx].Valid && row[queryIdx].Valid {
					value := row[i].String
					usename := row[usenameIdx].String
					datname := row[datnameIdx].String
					event := row[eventIdx].String
					query := row[queryIdx].String
					stats.updateMaxWaittimeDuration(value, usename, datname, event, query)
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
func (s *postgresActivityStat) updateMaxIdletimeDuration(value, usename, datname, state, query string) {
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
		log.Errorf("invalid input, parse '%s' failed: %s; skip", value, err.Error())
		return
	}

	// all validations ok, update stats

	// inspect query - is ia a user activity like queries, or maintenance tasks like automatic or regular vacuum/analyze.
	key := usename + "/" + datname

	if s.re.vacanl.MatchString(query) {
		if v > s.maxIdleMaint[key] {
			s.maxIdleMaint[key] = v
		}
	} else {
		if v > s.maxIdleUser[key] {
			s.maxIdleUser[key] = v
		}
	}
}

// updateMaxRuntimeDuration updates max duration of running activity.
func (s *postgresActivityStat) updateMaxRuntimeDuration(value, usename, datname, state, etype, query string) {
	// necessary values should not be empty (except wait_event_type)
	if value == "" || state == "" || query == "" {
		return
	}

	// don't account time for idle or blocked connections, interesting only for running activity.
	if state != stActive || etype == weLock {
		return
	}

	v, err := strconv.ParseFloat(value, 64)
	if err != nil {
		log.Errorf("invalid input, parse '%s' failed: %s; skip", value, err.Error())
		return
	}

	// all validations ok, update stats

	// inspect query - is ia a user activity like queries, or maintenance tasks like automatic or regular vacuum/analyze.
	key := usename + "/" + datname

	if s.re.vacanl.MatchString(query) {
		if v > s.maxRunMaint[key] {
			s.maxRunMaint[key] = v
		}
	} else {
		if v > s.maxRunUser[key] {
			s.maxRunUser[key] = v
		}
	}
}

// updateMaxWaittimeDuration updates max duration of waiting activity.
func (s *postgresActivityStat) updateMaxWaittimeDuration(value, usename, datname, waiting, query string) {
	if value == "" || waiting == "" || query == "" {
		return
	}

	// waiting activity is considered only with wait_event_type = 'Lock' (or waiting = 't')
	if waiting != weLock && waiting != "t" {
		return
	}

	v, err := strconv.ParseFloat(value, 64)
	if err != nil {
		log.Errorf("invalid input, parse '%s' failed: %s; skip", value, err.Error())
		return
	}

	// all validations ok, update stats
	key := usename + "/" + datname

	if s.re.vacanl.MatchString(query) {
		if v > s.maxWaitMaint[key] {
			s.maxWaitMaint[key] = v
		}
	} else {
		if v > s.maxWaitUser[key] {
			s.maxWaitUser[key] = v
		}
	}
}

func (s *postgresActivityStat) updateQueryStat(query string, state string) {
	if state != stActive {
		return
	}

	if s.re.selects.MatchString(query) {
		s.querySelect++
		return
	}

	if s.re.mod.MatchString(query) {
		s.queryMod++
		return
	}

	if s.re.ddl.MatchString(query) {
		s.queryDdl++
		return
	}

	if s.re.maint.MatchString(query) {
		s.queryMaint++
		return
	}

	str := s.re.vacuum.FindString(query)
	if str != "" {
		s.queryMaint++

		if strings.HasPrefix(str, "autovacuum:") && strings.Contains(str, "(to prevent wraparound)") {
			s.vacuumOps["wraparound"]++
			return
		}

		if strings.HasPrefix(str, "autovacuum:") {
			s.vacuumOps["regular"]++
			return
		}

		s.vacuumOps["user"]++
		return
	}

	if s.re.with.MatchString(query) {
		s.queryWith++
		return
	}

	if s.re.copy.MatchString(query) {
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
