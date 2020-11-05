package collector

import (
	"database/sql"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostgresActivityCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_activity_conn_total",
			"postgres_activity_max_seconds",
			"postgres_activity_prepared_xact_total",
			"postgres_activity_queries_in_flight",
			"postgres_activity_vacuums_total",
		},
		collector: NewPostgresActivityCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresActivityStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want postgresActivityStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 10,
				Ncols: 8,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("usename")},
					{Name: []byte("datname")},
					{Name: []byte("state")},
					{Name: []byte("wait_event_type")},
					{Name: []byte("wait_event")},
					{Name: []byte("since_start_seconds")},
					{Name: []byte("since_change_seconds")},
					{Name: []byte("query")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {}, {},
						{String: "10", Valid: true}, {String: "10", Valid: true}, {String: "SELECT active", Valid: true},
					},
					{
						{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "idle", Valid: true}, {}, {},
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
				},
			},
			want: postgresActivityStat{
				active: 6, idle: 1, idlexact: 3, other: 1, waiting: 2,
				maxIdleUser:  map[string]float64{"testuser/testdb": 20},
				maxIdleMaint: map[string]float64{"testuser/testdb": 28},
				maxRunUser:   map[string]float64{"testuser/testdb": 10},
				maxRunMaint:  map[string]float64{"testuser/testdb": 9},
				maxWaitUser:  map[string]float64{"testuser/testdb": 13},
				maxWaitMaint: map[string]float64{"testuser/testdb": 12},
				querySelect:  1, queryMod: 1, queryMaint: 4,
				vacuumOps: map[string]float64{"regular": 0, "user": 0, "wraparound": 0},
			},
		},
		{
			name: "queries",
			res: &model.PGResult{
				Nrows: 10,
				Ncols: 6,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("usename")},
					{Name: []byte("datname")},
					{Name: []byte("state")},
					{Name: []byte("wait_event_type")},
					{Name: []byte("wait_event")},
					{Name: []byte("since_start_seconds")},
					{Name: []byte("since_change_seconds")},
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
				maxIdleUser: map[string]float64{}, maxIdleMaint: map[string]float64{},
				maxRunUser: map[string]float64{"testuser/testdb": 1}, maxRunMaint: map[string]float64{"testuser/testdb": 1},
				maxWaitUser: map[string]float64{}, maxWaitMaint: map[string]float64{},
				active: 22, querySelect: 2, queryMod: 4, queryDdl: 3, queryMaint: 7, queryWith: 1, queryCopy: 1, queryOther: 4,
				vacuumOps: map[string]float64{"regular": 0, "user": 0, "wraparound": 0},
			},
		},
		{
			name: "old postgres with waiting instead of wait_event_type",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 6,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("usename")},
					{Name: []byte("datname")},
					{Name: []byte("state")},
					{Name: []byte("waiting")},
					{Name: []byte("since_start_seconds")},
					{Name: []byte("since_change_seconds")},
					{Name: []byte("query")},
				},
				Rows: [][]sql.NullString{
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {String: "t", Valid: true}, {String: "10", Valid: true}, {String: "5", Valid: true}, {String: "SELECT test 1", Valid: true}},
					{{String: "testuser", Valid: true}, {String: "testdb", Valid: true}, {String: "active", Valid: true}, {String: "f", Valid: true}, {String: "10", Valid: true}, {String: "10", Valid: true}, {String: "SELECT test 2", Valid: true}},
				},
			},
			want: postgresActivityStat{
				maxIdleUser: map[string]float64{}, maxIdleMaint: map[string]float64{},
				maxRunUser: map[string]float64{"testuser/testdb": 10}, maxRunMaint: map[string]float64{},
				maxWaitUser: map[string]float64{"testuser/testdb": 5}, maxWaitMaint: map[string]float64{},
				active: 2, waiting: 1, querySelect: 2,
				vacuumOps: map[string]float64{"regular": 0, "user": 0, "wraparound": 0},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresActivityStats(tc.res)
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
		{version: PostgresV96, want: postgresActivityQueryLatest},
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.want, selectActivityQuery(tc.version))
	}
}
