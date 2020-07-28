package collector

import (
	"database/sql"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostgresReplicationCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_recovery_state",
			"postgres_wal_bytes_total",
			"postgres_replication_lag_bytes",
			"postgres_replication_lag_seconds",
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
		res  *store.QueryResult
		want map[string]postgresReplicationStat
	}{
		{
			name: "normal output",
			res: &store.QueryResult{
				Nrows: 1,
				Ncols: 15,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("pid")}, {Name: []byte("client_addr")}, {Name: []byte("usename")}, {Name: []byte("application_name")}, {Name: []byte("state")},
					{Name: []byte("recovery")}, {Name: []byte("wal_bytes")},
					{Name: []byte("pending_lag_bytes")}, {Name: []byte("write_lag_bytes")}, {Name: []byte("flush_lag_bytes")},
					{Name: []byte("replay_lag_bytes")}, {Name: []byte("total_lag_bytes")}, {Name: []byte("write_lag_seconds")},
					{Name: []byte("flush_lag_seconds")}, {Name: []byte("replay_lag_seconds")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "123456", Valid: true}, {String: "127.0.0.1", Valid: true}, {String: "testuser", Valid: true}, {String: "testapp", Valid: true},
						{String: "teststate", Valid: true}, {String: "0", Valid: true}, {String: "999999", Valid: true},
						{String: "100", Valid: true}, {String: "200", Valid: true}, {String: "300", Valid: true}, {String: "400", Valid: true},
						{String: "500", Valid: true}, {String: "600", Valid: true}, {String: "700", Valid: true}, {String: "800", Valid: true},
					},
				},
			},
			want: map[string]postgresReplicationStat{
				"123456": {
					pid: "123456", clientaddr: "127.0.0.1", usename: "testuser", applicationName: "testapp", state: "teststate",
					recovery: 0, walBytes: 999999, pendingLagBytes: 100, writeLagBytes: 200, flushLagBytes: 300, replayLagBytes: 400,
					totalLagBytes: 500, writeLagSeconds: 600, flushLagSeconds: 700, replayLagSeconds: 800,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresReplicationStats(tc.res, []string{"client_addr", "usename", "application_name", "state", "type"})
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
