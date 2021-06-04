package collector

import (
	"database/sql"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/model"
	"testing"
)

func TestPostgresActivityCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_up",
			"postgres_start_time_seconds",
			"postgres_activity_wait_events_in_flight",
			"postgres_activity_connections_in_flight",
			"postgres_activity_connections_all_in_flight",
			"postgres_activity_max_seconds",
			"postgres_activity_prepared_transactions_in_flight",
			"postgres_activity_queries_in_flight",
			"postgres_activity_vacuums_in_flight",
		},
		collector: NewPostgresActivityCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresActivityStats(t *testing.T) {
	testRE := newQueryRegexp()

	var testCases = []struct {
		name string
		res  *model.PGResult
		want postgresActivityStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 11,
				Ncols: 8,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("user")},
					{Name: []byte("database")},
					{Name: []byte("state")},
					{Name: []byte("wait_event_type")},
					{Name: []byte("wait_event")},
					{Name: []byte("active_seconds")},
					{Name: []byte("waiting_seconds")},
					{Name: []byte("query")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {},
						{String: "10", Valid: true}, {String: "10", Valid: true}, {String: "SELECT active", Valid: true},
					},
					{
						{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "idle", Valid: true},
						{String: "Client", Valid: true}, {String: "ClientRead", Valid: true},
						{String: "100", Valid: true}, {String: "100", Valid: true}, {String: "SELECT idle", Valid: true},
					},
					{
						{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "fastpath function call", Valid: true}, {}, {},
						{String: "1", Valid: true}, {String: "1", Valid: true}, {String: "SELECT fastpath", Valid: true},
					},
					{
						{String: "testuser", Valid: true}, {String: "testdb", Valid: true},
						{String: "idle in transaction", Valid: true}, {String: "Client", Valid: true}, {String: "ClientRead", Valid: true},
						{String: "20", Valid: true}, {String: "20", Valid: true}, {String: "SELECT idle in transaction", Valid: true},
					},
					{
						{String: "testuser", Valid: true}, {String: "testdb", Valid: true},
						{String: "idle in transaction", Valid: true}, {String: "Client", Valid: true}, {String: "ClientRead", Valid: true},
						{String: "28", Valid: true}, {String: "18", Valid: true}, {String: "ANALYZE example", Valid: true},
					},
					{
						{String: "testuser", Valid: true}, {String: "testdb", Valid: true},
						{String: "idle in transaction (aborted)", Valid: true}, {String: "Client", Valid: true}, {String: "ClientRead", Valid: true},
						{String: "15", Valid: true}, {String: "15", Valid: true}, {String: "SELECT idle in transaction", Valid: true},
					},
					{
						{String: "testuser", Valid: true}, {String: "testdb", Valid: true},
						{String: "active", Valid: true}, {}, {}, {String: "5", Valid: true}, {String: "5", Valid: true}, {String: "VACUUM example1", Valid: true},
					},
					{
						{String: "testuser", Valid: true}, {String: "testdb", Valid: true},
						{String: "active", Valid: true}, {}, {}, {String: "7", Valid: true}, {String: "6", Valid: true}, {String: "analyze example2", Valid: true},
					},
					{
						{String: "testuser", Valid: true}, {String: "testdb", Valid: true},
						{String: "active", Valid: true}, {}, {}, {String: "9", Valid: true}, {String: "7", Valid: true}, {String: "autovacuum: VACUUM example3", Valid: true},
					},
					{
						{String: "testuser", Valid: true}, {String: "testdb", Valid: true},
						{String: "active", Valid: true}, {String: "Lock", Valid: true}, {String: "transactionid", Valid: true},
						{String: "20", Valid: true}, {String: "13", Valid: true}, {String: "UPDATE waiting", Valid: true},
					},
					{
						{String: "testuser", Valid: true}, {String: "testdb", Valid: true},
						{String: "active", Valid: true}, {String: "Lock", Valid: true}, {String: "transactionid", Valid: true},
						{String: "12", Valid: true}, {String: "12", Valid: true}, {String: "VACUUM example2", Valid: true},
					},
					{ // this is a WAL sender - its state is active, but not accounted.
						{String: "postgres", Valid: true}, {String: "", Valid: false},
						{String: "active", Valid: true}, {String: "", Valid: false}, {String: "", Valid: false},
						{String: "0", Valid: true}, {String: "0", Valid: true}, {String: "START_REPLICATION", Valid: true},
					},
				},
			},
			want: postgresActivityStat{
				active: 4, idle: 1, idlexact: 3, other: 1, waiting: 2,
				waitEvents:     map[string]float64{"Client/ClientRead": 4, "Lock/transactionid": 2},
				maxIdleUser:    map[string]float64{"testuser/testdb": 20},
				maxIdleMaint:   map[string]float64{"testuser/testdb": 28},
				maxActiveUser:  map[string]float64{"testuser/testdb": 10},
				maxActiveMaint: map[string]float64{"testuser/testdb": 9},
				maxWaitUser:    map[string]float64{"testuser/testdb": 13},
				maxWaitMaint:   map[string]float64{"testuser/testdb": 12},
				querySelect:    1, queryMod: 1, queryMaint: 4, queryOther: 1,
				vacuumOps: map[string]float64{"regular": 1, "user": 2, "wraparound": 0},
				re:        testRE,
			},
		},
		{
			name: "queries",
			res: &model.PGResult{
				Nrows: 10,
				Ncols: 6,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("user")},
					{Name: []byte("database")},
					{Name: []byte("state")},
					{Name: []byte("wait_event_type")},
					{Name: []byte("wait_event")},
					{Name: []byte("active_seconds")},
					{Name: []byte("waiting_seconds")},
					{Name: []byte("query")},
				},
				Rows: [][]sql.NullString{
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "SELECT test", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "TABLE test", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "INSERT test", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "UPDATE test", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "DELETE test", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "TRUNCATE test", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "CREATE test", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "ALTER test", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "DROP test", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "VACUUM test", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "ANALYZE test", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "CLUSTER test", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "REINDEX test", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "REFRESH test", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "CHECKPOINT", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "autovacuum: VACUUM test", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "WITH (test)", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "COPY test", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "SET test", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "SHOW test", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "BEGIN test", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "COMMIT test", Valid: true}},
				},
			},
			want: postgresActivityStat{
				waitEvents:  map[string]float64{},
				maxIdleUser: map[string]float64{}, maxIdleMaint: map[string]float64{},
				maxActiveUser: map[string]float64{"testuser/testdb": 1}, maxActiveMaint: map[string]float64{"testuser/testdb": 1},
				maxWaitUser: map[string]float64{}, maxWaitMaint: map[string]float64{},
				active: 22, querySelect: 2, queryMod: 4, queryDdl: 3, queryMaint: 7, queryWith: 1, queryCopy: 1, queryOther: 4,
				vacuumOps: map[string]float64{"regular": 1, "user": 1, "wraparound": 0},
				re:        testRE,
			},
		},
		{
			name: "old postgres with waiting instead of wait_event_type",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 6,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("user")},
					{Name: []byte("database")},
					{Name: []byte("state")},
					{Name: []byte("waiting")},
					{Name: []byte("active_seconds")},
					{Name: []byte("waiting_seconds")},
					{Name: []byte("query")},
				},
				Rows: [][]sql.NullString{
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {String: "t", Valid: true}, {String: "10", Valid: true}, {String: "5", Valid: true}, {String: "SELECT test 1", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {String: "f", Valid: true}, {String: "10", Valid: true}, {String: "10", Valid: true}, {String: "SELECT test 2", Valid: true}},
				},
			},
			want: postgresActivityStat{
				waitEvents:  map[string]float64{},
				maxIdleUser: map[string]float64{}, maxIdleMaint: map[string]float64{},
				maxActiveUser: map[string]float64{"testuser/testdb": 10}, maxActiveMaint: map[string]float64{},
				maxWaitUser: map[string]float64{"testuser/testdb": 5}, maxWaitMaint: map[string]float64{},
				active: 1, waiting: 1, querySelect: 2,
				vacuumOps: map[string]float64{"regular": 0, "user": 0, "wraparound": 0},
				re:        testRE,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresActivityStats(tc.res, testRE)
			assert.EqualValues(t, tc.want, got)
		})
	}
}

func Test_selectActivityQuery(t *testing.T) {
	testcases := []struct {
		version int
		want    string
	}{
		{version: PostgresV95, want: postgresActivityQuery95},
		{version: PostgresV96, want: postgresActivityQuery96},
		{version: PostgresV10, want: postgresActivityQueryLatest},
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.want, selectActivityQuery(tc.version))
	}
}

func Test_updateMaxIdletimeDuration(t *testing.T) {
	testRE := newQueryRegexp()

	testcases := []struct {
		value   string
		usename string
		datname string
		state   string
		query   string
		want    postgresActivityStat
	}{
		{value: "1", usename: "", datname: "", state: "", query: "",
			want: newPostgresActivityStat(testRE),
		},
		{value: "10", usename: "testuser", datname: "testdb", state: "active", query: "UPDATE table",
			want: newPostgresActivityStat(testRE),
		},
		{value: "qq", usename: "testuser", datname: "testdb", state: "idle in transaction", query: "UPDATE table",
			want: newPostgresActivityStat(testRE),
		},
		{value: "10", usename: "testuser", datname: "testdb", state: "idle in transaction", query: "UPDATE table",
			want: postgresActivityStat{
				waitEvents:  map[string]float64{},
				maxIdleUser: map[string]float64{"testuser/testdb": 10}, maxIdleMaint: map[string]float64{},
				maxActiveUser: map[string]float64{}, maxActiveMaint: map[string]float64{},
				maxWaitUser: map[string]float64{}, maxWaitMaint: map[string]float64{},
				vacuumOps: map[string]float64{"regular": 0, "user": 0, "wraparound": 0},
				re:        testRE,
			},
		},
		{value: "10", usename: "testuser", datname: "testdb", state: "idle in transaction", query: "autovacuum: VACUUM table",
			want: postgresActivityStat{
				waitEvents:  map[string]float64{},
				maxIdleUser: map[string]float64{}, maxIdleMaint: map[string]float64{"testuser/testdb": 10},
				maxActiveUser: map[string]float64{}, maxActiveMaint: map[string]float64{},
				maxWaitUser: map[string]float64{}, maxWaitMaint: map[string]float64{},
				vacuumOps: map[string]float64{"regular": 0, "user": 0, "wraparound": 0},
				re:        testRE,
			},
		},
		{value: "10", usename: "testuser", datname: "testdb", state: "idle in transaction", query: "VACUUM table",
			want: postgresActivityStat{
				waitEvents:  map[string]float64{},
				maxIdleUser: map[string]float64{}, maxIdleMaint: map[string]float64{"testuser/testdb": 10},
				maxActiveUser: map[string]float64{}, maxActiveMaint: map[string]float64{},
				maxWaitUser: map[string]float64{}, maxWaitMaint: map[string]float64{},
				vacuumOps: map[string]float64{"regular": 0, "user": 0, "wraparound": 0},
				re:        testRE,
			},
		},
	}

	for _, tc := range testcases {
		s := newPostgresActivityStat(testRE)
		s.updateMaxIdletimeDuration(tc.value, tc.usename, tc.datname, tc.state, tc.query)
		assert.Equal(t, tc.want, s)
	}
}

func Test_updateMaxRuntimeDuration(t *testing.T) {
	testRE := newQueryRegexp()

	testcases := []struct {
		value   string
		usename string
		datname string
		state   string
		etype   string
		query   string
		want    postgresActivityStat
	}{
		{value: "1", usename: "", datname: "", state: "", etype: "", query: "",
			want: newPostgresActivityStat(testRE),
		},
		{value: "2", usename: "testuser", datname: "testdb", state: "idle", etype: "Client", query: "UPDATE table",
			want: newPostgresActivityStat(testRE),
		},
		{value: "3", usename: "testuser", datname: "testdb", state: "active", etype: "Lock", query: "UPDATE table",
			want: newPostgresActivityStat(testRE),
		},
		{value: "qq", usename: "testuser", datname: "testdb", state: "active", etype: "", query: "UPDATE table",
			want: newPostgresActivityStat(testRE),
		},
		{value: "4", usename: "testuser", datname: "testdb", state: "idle in transaction", etype: "", query: "UPDATE table",
			want: newPostgresActivityStat(testRE),
		},
		{value: "5", usename: "testuser", datname: "testdb", state: "active", query: "UPDATE table",
			want: postgresActivityStat{
				waitEvents:  map[string]float64{},
				maxIdleUser: map[string]float64{}, maxIdleMaint: map[string]float64{},
				maxActiveUser: map[string]float64{"testuser/testdb": 5}, maxActiveMaint: map[string]float64{},
				maxWaitUser: map[string]float64{}, maxWaitMaint: map[string]float64{},
				vacuumOps: map[string]float64{"regular": 0, "user": 0, "wraparound": 0},
				re:        testRE,
			},
		},
		{value: "6", usename: "testuser", datname: "testdb", state: "active", query: "autovacuum: VACUUM table",
			want: postgresActivityStat{
				waitEvents:  map[string]float64{},
				maxIdleUser: map[string]float64{}, maxIdleMaint: map[string]float64{},
				maxActiveUser: map[string]float64{}, maxActiveMaint: map[string]float64{"testuser/testdb": 6},
				maxWaitUser: map[string]float64{}, maxWaitMaint: map[string]float64{},
				vacuumOps: map[string]float64{"regular": 0, "user": 0, "wraparound": 0},
				re:        testRE,
			},
		},
	}

	for _, tc := range testcases {
		s := newPostgresActivityStat(testRE)
		s.updateMaxRuntimeDuration(tc.value, tc.usename, tc.datname, tc.state, tc.etype, tc.query)
		assert.Equal(t, tc.want, s)
	}
}

func Test_updateMaxWaittimeDuration(t *testing.T) {
	testRE := newQueryRegexp()

	testcases := []struct {
		value   string
		usename string
		datname string
		waiting string
		query   string
		want    postgresActivityStat
	}{
		{value: "1", usename: "", datname: "", waiting: "", query: "",
			want: newPostgresActivityStat(testRE),
		},
		{value: "2", usename: "testuser", datname: "testdb", waiting: "Client", query: "UPDATE table",
			want: newPostgresActivityStat(testRE),
		},
		{value: "qq", usename: "testuser", datname: "testdb", waiting: "Lock", query: "UPDATE table",
			want: newPostgresActivityStat(testRE),
		},
		{value: "5", usename: "testuser", datname: "testdb", waiting: "Lock", query: "UPDATE table",
			want: postgresActivityStat{
				waitEvents:  map[string]float64{},
				maxIdleUser: map[string]float64{}, maxIdleMaint: map[string]float64{},
				maxActiveUser: map[string]float64{}, maxActiveMaint: map[string]float64{},
				maxWaitUser: map[string]float64{"testuser/testdb": 5}, maxWaitMaint: map[string]float64{},
				vacuumOps: map[string]float64{"regular": 0, "user": 0, "wraparound": 0},
				re:        testRE,
			},
		},
		{value: "6", usename: "testuser", datname: "testdb", waiting: "t", query: "autovacuum: VACUUM table",
			want: postgresActivityStat{
				waitEvents:  map[string]float64{},
				maxIdleUser: map[string]float64{}, maxIdleMaint: map[string]float64{},
				maxActiveUser: map[string]float64{}, maxActiveMaint: map[string]float64{},
				maxWaitUser: map[string]float64{}, maxWaitMaint: map[string]float64{"testuser/testdb": 6},
				vacuumOps: map[string]float64{"regular": 0, "user": 0, "wraparound": 0},
				re:        testRE,
			},
		},
	}

	for _, tc := range testcases {
		s := newPostgresActivityStat(testRE)
		s.updateMaxWaittimeDuration(tc.value, tc.usename, tc.datname, tc.waiting, tc.query)
		assert.Equal(t, tc.want, s)
	}
}

func Test_updateQueryStat(t *testing.T) {
	testRE := newQueryRegexp()

	queries := []string{
		"SELECT test", "TABLE test", "test SELECT test", "test TABLE test",
		"INSERT test", "UPDATE test", "DELETE test", "TRUNCATE test",
		"test INSERT test", "test UPDATE test", "test DELETE test", "test TRUNCATE test",
		"CREATE test", "ALTER test", "DROP test", "test CREATE test", "test ALTER test", "test DROP test",
		"ANALYZE test", "CLUSTER test", "REINDEX test", "REFRESH test", "CHECKPOINT test",
		"test ANALYZE test", "test CLUSTER test", "test REINDEX test", "test REFRESH test", "test CHECKPOINT test",
		"VACUUM test", "autovacuum: VACUUM test", "autovacuum: ANALYZE test", "autovacuum: VACUUM test (to prevent wraparound)",
		"test VACUUM test", "test autovacuum: VACUUM test", "test autovacuum: ANALYZE test", "test autovacuum: VACUUM test (to prevent wraparound)",
		"WITH qq AS test", "COPY test", "test WITH qq AS test", "test COPY test",
	}

	s := newPostgresActivityStat(testRE)
	s.updateQueryStat("SELECT 1", "idle")
	assert.Equal(t, newPostgresActivityStat(testRE), s)

	for _, q := range queries {
		s.updateQueryStat(q, "active")
	}

	assert.Equal(t, postgresActivityStat{
		waitEvents:  map[string]float64{},
		maxIdleUser: map[string]float64{}, maxIdleMaint: map[string]float64{},
		maxActiveUser: map[string]float64{}, maxActiveMaint: map[string]float64{},
		maxWaitUser: map[string]float64{}, maxWaitMaint: map[string]float64{},
		querySelect: 2,
		queryMod:    4,
		queryDdl:    3,
		queryMaint:  9,
		queryWith:   1,
		queryCopy:   1,
		queryOther:  20,
		vacuumOps:   map[string]float64{"regular": 2, "user": 1, "wraparound": 1},
		re:          testRE,
	}, s)
}
