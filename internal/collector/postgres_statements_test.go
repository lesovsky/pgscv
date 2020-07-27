package collector

import (
	"database/sql"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostgresStatementsCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_statements_calls_total",
			"postgres_statements_rows_total",
			"postgres_statements_time_total",
			"postgres_statements_blocks_total",
		},
		optional:  []string{},
		collector: NewPostgresStatementsCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresStatementsStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *store.QueryResult
		want map[string]postgresStatementsStat
	}{
		{
			name: "normal output",
			res: &store.QueryResult{
				Nrows: 1,
				Ncols: 19,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("datname")}, {Name: []byte("usename")}, {Name: []byte("queryid")}, {Name: []byte("query")},
					{Name: []byte("calls")}, {Name: []byte("rows")},
					{Name: []byte("total_time")}, {Name: []byte("blk_read_time")}, {Name: []byte("blk_write_time")},
					{Name: []byte("shared_blks_hit")}, {Name: []byte("shared_blks_read")}, {Name: []byte("shared_blks_dirtied")}, {Name: []byte("shared_blks_written")},
					{Name: []byte("local_blks_hit")}, {Name: []byte("local_blks_read")}, {Name: []byte("local_blks_dirtied")}, {Name: []byte("local_blks_written")},
					{Name: []byte("temp_blks_read")}, {Name: []byte("temp_blks_written")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testdb", Valid: true}, {String: "testuser", Valid: true}, {String: "-1856485172033541804", Valid: true}, {String: "SELECT test", Valid: true},
						{String: "1000", Valid: true}, {String: "2000", Valid: true},
						{String: "30000", Valid: true}, {String: "6000", Valid: true}, {String: "4000", Valid: true},
						{String: "100", Valid: true}, {String: "110", Valid: true}, {String: "120", Valid: true}, {String: "130", Valid: true},
						{String: "500", Valid: true}, {String: "510", Valid: true}, {String: "520", Valid: true}, {String: "530", Valid: true},
						{String: "700", Valid: true}, {String: "710", Valid: true},
					},
				},
			},
			want: map[string]postgresStatementsStat{
				"testdb/testuser/-1856485172033541804": {
					datname: "testdb", usename: "testuser", queryid: "-1856485172033541804", query: "SELECT test",
					calls: 1000, rows: 2000,
					totalTime: 30000, blkReadTime: 6000, blkWriteTime: 4000,
					sharedBlksHit: 100, sharedBlksRead: 110, sharedBlksDirtied: 120, sharedBlksWritten: 130,
					localBlksHit: 500, localBlksRead: 510, localBlksDirtied: 520, localBlksWritten: 530,
					tempBlksRead: 700, tempBlksWritten: 710,
				},
			},
		},
		{
			name: "lot of nulls and unknown columns",
			res: &store.QueryResult{
				Nrows: 1,
				Ncols: 20,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("datname")}, {Name: []byte("usename")}, {Name: []byte("queryid")}, {Name: []byte("query")},
					{Name: []byte("calls")}, {Name: []byte("rows")},
					{Name: []byte("total_time")}, {Name: []byte("blk_read_time")}, {Name: []byte("blk_write_time")}, {Name: []byte("min_time")},
					{Name: []byte("shared_blks_hit")}, {Name: []byte("shared_blks_read")}, {Name: []byte("shared_blks_dirtied")}, {Name: []byte("shared_blks_written")},
					{Name: []byte("local_blks_hit")}, {Name: []byte("local_blks_read")}, {Name: []byte("local_blks_dirtied")}, {Name: []byte("local_blks_written")},
					{Name: []byte("temp_blks_read")}, {Name: []byte("temp_blks_written")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testdb", Valid: true}, {String: "testuser", Valid: true}, {String: "-1856485172033541804", Valid: true}, {String: "SELECT test", Valid: true},
						{String: "1000", Valid: true}, {String: "2000", Valid: true},
						{String: "30000", Valid: true}, {String: "6000", Valid: true}, {String: "4000", Valid: true}, {String: "100", Valid: true},
						{}, {}, {}, {}, {}, {}, {}, {}, {}, {},
					},
				},
			},
			want: map[string]postgresStatementsStat{
				"testdb/testuser/-1856485172033541804": {
					datname: "testdb", usename: "testuser", queryid: "-1856485172033541804", query: "SELECT test",
					calls: 1000, rows: 2000,
					totalTime: 30000, blkReadTime: 6000, blkWriteTime: 4000,
					sharedBlksHit: 0, sharedBlksRead: 0, sharedBlksDirtied: 0, sharedBlksWritten: 0,
					localBlksHit: 0, localBlksRead: 0, localBlksDirtied: 0, localBlksWritten: 0,
					tempBlksRead: 0, tempBlksWritten: 0,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresStatementsStats(tc.res, []string{"usename", "datname", "queryid", "query"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}
