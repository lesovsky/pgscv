package collector

import (
	"database/sql"
	"github.com/jackc/pgproto3/v2"
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostgresWalCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_recovery_info",
			"postgres_wal_written_bytes_total",
		},
		// TODO: wait until Postgres 14 has been released, update Postgres version on pgscv-testing docker image
		//   and move these metrics to 'required' slice.
		optional: []string{
			"postgres_wal_records_total",
			"postgres_wal_fpi_total",
			"postgres_wal_bytes_total",
			"postgres_wal_buffers_full_total",
			"postgres_wal_write_total",
			"postgres_wal_sync_total",
			"postgres_wal_seconds_all_total",
			"postgres_wal_seconds_total",
			"postgres_wal_stats_reset_time",
		},
		collector: NewPostgresWalCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresWalStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]float64
	}{
		{
			name: "pg14",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 11,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("recovery")},
					{Name: []byte("wal_records")}, {Name: []byte("wal_fpi")}, {Name: []byte("wal_bytes")}, {Name: []byte("wal_written")},
					{Name: []byte("wal_buffers_full")}, {Name: []byte("wal_write")}, {Name: []byte("wal_sync")},
					{Name: []byte("wal_write_time")}, {Name: []byte("wal_sync_time")}, {Name: []byte("reset_time")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "0", Valid: true},
						{String: "58452", Valid: true}, {String: "4712", Valid: true}, {String: "587241", Valid: true}, {String: "8746951", Valid: true},
						{String: "1234", Valid: true}, {String: "48541", Valid: true}, {String: "8541", Valid: true},
						{String: "874215", Valid: true}, {String: "48736", Valid: true}, {String: "123456789", Valid: true},
					},
				},
			},
			want: map[string]float64{
				"recovery":    0,
				"wal_records": 58452, "wal_fpi": 4712, "wal_bytes": 587241, "wal_written": 8746951,
				"wal_buffers_full": 1234, "wal_write": 48541, "wal_sync": 8541,
				"wal_write_time": 874215, "wal_sync_time": 48736, "wal_all_time": 922951, "reset_time": 123456789,
			},
		},
		{
			name: "pg13",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 2,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("recovery")}, {Name: []byte("wal_written")},
				},
				Rows: [][]sql.NullString{{{String: "0", Valid: true}, {String: "123456789", Valid: true}}},
			},
			want: map[string]float64{"recovery": 0, "wal_written": 123456789},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresWalStats(tc.res)
			assert.EqualValues(t, tc.want, got)
		})
	}
}

func Test_selectWalQuery(t *testing.T) {
	var testcases = []struct {
		version int
		want    string
	}{
		{version: 90600, want: postgresWalQuery96},
		{version: 90605, want: postgresWalQuery96},
		{version: 100000, want: postgresWalQuery13},
		{version: 100005, want: postgresWalQuery13},
		{version: 130005, want: postgresWalQuery13},
		{version: 140005, want: postgresWalQueryLatest},
	}

	for _, tc := range testcases {
		t.Run("", func(t *testing.T) {
			assert.Equal(t, tc.want, selectWalQuery(tc.version))
		})
	}
}
