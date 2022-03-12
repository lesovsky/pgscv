package collector

import (
	"database/sql"
	"github.com/jackc/pgproto3/v2"
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostgresReplicationCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_replication_lag_bytes",
			"postgres_replication_lag_all_bytes",
			"postgres_replication_lag_seconds",
			"postgres_replication_lag_all_seconds",
		},
		optional:  []string{},
		collector: NewPostgresReplicationCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresReplicationStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]postgresReplicationStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 14,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("pid")}, {Name: []byte("client_addr")}, {Name: []byte("user")}, {Name: []byte("application_name")}, {Name: []byte("state")},
					{Name: []byte("pending_lag_bytes")}, {Name: []byte("write_lag_bytes")}, {Name: []byte("flush_lag_bytes")},
					{Name: []byte("replay_lag_bytes")}, {Name: []byte("total_lag_bytes")}, {Name: []byte("write_lag_seconds")},
					{Name: []byte("flush_lag_seconds")}, {Name: []byte("replay_lag_seconds")}, {Name: []byte("total_lag_seconds")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "123456", Valid: true}, {String: "127.0.0.1", Valid: true}, {String: "testuser", Valid: true}, {String: "testapp", Valid: true},
						{String: "teststate", Valid: true},
						{String: "100", Valid: true}, {String: "200", Valid: true}, {String: "300", Valid: true}, {String: "400", Valid: true},
						{String: "500", Valid: true}, {String: "600", Valid: true}, {String: "700", Valid: true}, {String: "800", Valid: true}, {String: "2100", Valid: true},
					},
					{
						// pg_receivewals and pg_basebackups don't have replay lag.
						{String: "101010", Valid: true}, {String: "127.0.0.1", Valid: true}, {String: "testuser", Valid: true}, {String: "pg_receivewal", Valid: true},
						{String: "teststate", Valid: true},
						{String: "4257", Valid: true}, {String: "8452", Valid: true}, {String: "5785", Valid: true}, {String: "", Valid: false},
						{String: "", Valid: false}, {String: "2458", Valid: true}, {String: "7871", Valid: true}, {String: "6896", Valid: true}, {String: "17225", Valid: true},
					},
				},
			},
			want: map[string]postgresReplicationStat{
				"123456": {
					pid: "123456", clientaddr: "127.0.0.1", user: "testuser", applicationName: "testapp", state: "teststate",
					values: map[string]float64{
						"pending_lag_bytes": 100, "write_lag_bytes": 200, "flush_lag_bytes": 300, "replay_lag_bytes": 400, "total_lag_bytes": 500,
						"write_lag_seconds": 600, "flush_lag_seconds": 700, "replay_lag_seconds": 800, "total_lag_seconds": 2100,
					},
				},
				"101010": {
					pid: "101010", clientaddr: "127.0.0.1", user: "testuser", applicationName: "pg_receivewal", state: "teststate",
					values: map[string]float64{
						"pending_lag_bytes": 4257, "write_lag_bytes": 8452, "flush_lag_bytes": 5785,
						"write_lag_seconds": 2458, "flush_lag_seconds": 7871, "replay_lag_seconds": 6896, "total_lag_seconds": 17225,
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresReplicationStats(tc.res, []string{"client_addr", "user", "application_name", "state", "type"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}

func Test_selectReplicationQuery(t *testing.T) {
	var testcases = []struct {
		version int
		want    string
	}{
		{version: 90600, want: postgresReplicationQuery96},
		{version: 90605, want: postgresReplicationQuery96},
		{version: 100000, want: postgresReplicationQueryLatest},
		{version: 100005, want: postgresReplicationQueryLatest},
	}

	for _, tc := range testcases {
		t.Run("", func(t *testing.T) {
			assert.Equal(t, tc.want, selectReplicationQuery(tc.version))
		})
	}
}
