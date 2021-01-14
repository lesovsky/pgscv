package collector

import (
	"bufio"
	"database/sql"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestPostgresStatementsCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_statements_query_info",
			"postgres_statements_calls_total",
			"postgres_statements_rows_total",
			"postgres_statements_time_seconds_total",
			"postgres_statements_time_seconds_all_total",
		},
		optional: []string{
			"postgres_statements_shared_hits_total",
			"postgres_statements_shared_read_bytes_total",
			"postgres_statements_shared_dirtied_bytes_total",
			"postgres_statements_shared_written_bytes_total",
			"postgres_statements_local_hits_total",
			"postgres_statements_local_read_bytes_total",
			"postgres_statements_local_dirtied_bytes_total",
			"postgres_statements_local_written_bytes_total",
			"postgres_statements_temp_read_bytes_total",
			"postgres_statements_temp_written_bytes_total",
		},
		collector: NewPostgresStatementsCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresStatementsStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]postgresStatementStat
	}{
		{
			name: "normal output, Postgres 12",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 18,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("datname")}, {Name: []byte("usename")}, {Name: []byte("query")},
					{Name: []byte("calls")}, {Name: []byte("rows")},
					{Name: []byte("total_time")}, {Name: []byte("blk_read_time")}, {Name: []byte("blk_write_time")},
					{Name: []byte("shared_blks_hit")}, {Name: []byte("shared_blks_read")}, {Name: []byte("shared_blks_dirtied")}, {Name: []byte("shared_blks_written")},
					{Name: []byte("local_blks_hit")}, {Name: []byte("local_blks_read")}, {Name: []byte("local_blks_dirtied")}, {Name: []byte("local_blks_written")},
					{Name: []byte("temp_blks_read")}, {Name: []byte("temp_blks_written")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testdb", Valid: true}, {String: "testuser", Valid: true}, {String: "SELECT test", Valid: true},
						{String: "1000", Valid: true}, {String: "2000", Valid: true},
						{String: "30000", Valid: true}, {String: "6000", Valid: true}, {String: "4000", Valid: true},
						{String: "100", Valid: true}, {String: "110", Valid: true}, {String: "120", Valid: true}, {String: "130", Valid: true},
						{String: "500", Valid: true}, {String: "510", Valid: true}, {String: "520", Valid: true}, {String: "530", Valid: true},
						{String: "700", Valid: true}, {String: "710", Valid: true},
					},
				},
			},
			want: map[string]postgresStatementStat{
				"testdb/testuser/c5ae4e2c19d733cda381b2dc24db57d7": {
					datname: "testdb", usename: "testuser", md5hash: "c5ae4e2c19d733cda381b2dc24db57d7", query: "SELECT test",
					calls: 1000, rows: 2000,
					totalExecTime: 30000, blkReadTime: 6000, blkWriteTime: 4000,
					sharedBlksHit: 100, sharedBlksRead: 110, sharedBlksDirtied: 120, sharedBlksWritten: 130,
					localBlksHit: 500, localBlksRead: 510, localBlksDirtied: 520, localBlksWritten: 530,
					tempBlksRead: 700, tempBlksWritten: 710,
				},
			},
		},
		{
			name: "normal output, Postgres 13",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 19,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("datname")}, {Name: []byte("usename")}, {Name: []byte("query")},
					{Name: []byte("calls")}, {Name: []byte("rows")},
					{Name: []byte("total_exec_time")}, {Name: []byte("total_plan_time")}, {Name: []byte("blk_read_time")}, {Name: []byte("blk_write_time")},
					{Name: []byte("shared_blks_hit")}, {Name: []byte("shared_blks_read")}, {Name: []byte("shared_blks_dirtied")}, {Name: []byte("shared_blks_written")},
					{Name: []byte("local_blks_hit")}, {Name: []byte("local_blks_read")}, {Name: []byte("local_blks_dirtied")}, {Name: []byte("local_blks_written")},
					{Name: []byte("temp_blks_read")}, {Name: []byte("temp_blks_written")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testdb", Valid: true}, {String: "testuser", Valid: true}, {String: "SELECT test", Valid: true},
						{String: "1000", Valid: true}, {String: "2000", Valid: true},
						{String: "30000", Valid: true}, {String: "100", Valid: true}, {String: "6000", Valid: true}, {String: "4000", Valid: true},
						{String: "100", Valid: true}, {String: "110", Valid: true}, {String: "120", Valid: true}, {String: "130", Valid: true},
						{String: "500", Valid: true}, {String: "510", Valid: true}, {String: "520", Valid: true}, {String: "530", Valid: true},
						{String: "700", Valid: true}, {String: "710", Valid: true},
					},
				},
			},
			want: map[string]postgresStatementStat{
				"testdb/testuser/c5ae4e2c19d733cda381b2dc24db57d7": {
					datname: "testdb", usename: "testuser", md5hash: "c5ae4e2c19d733cda381b2dc24db57d7", query: "SELECT test",
					calls: 1000, rows: 2000,
					totalExecTime: 30000, totalPlanTime: 100, blkReadTime: 6000, blkWriteTime: 4000,
					sharedBlksHit: 100, sharedBlksRead: 110, sharedBlksDirtied: 120, sharedBlksWritten: 130,
					localBlksHit: 500, localBlksRead: 510, localBlksDirtied: 520, localBlksWritten: 530,
					tempBlksRead: 700, tempBlksWritten: 710,
				},
			},
		},
		{
			name: "lot of nulls and unknown columns",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 20,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("datname")}, {Name: []byte("usename")}, {Name: []byte("query")},
					{Name: []byte("calls")}, {Name: []byte("rows")},
					{Name: []byte("total_exec_time")}, {Name: []byte("total_plan_time")}, {Name: []byte("blk_read_time")}, {Name: []byte("blk_write_time")}, {Name: []byte("min_time")},
					{Name: []byte("shared_blks_hit")}, {Name: []byte("shared_blks_read")}, {Name: []byte("shared_blks_dirtied")}, {Name: []byte("shared_blks_written")},
					{Name: []byte("local_blks_hit")}, {Name: []byte("local_blks_read")}, {Name: []byte("local_blks_dirtied")}, {Name: []byte("local_blks_written")},
					{Name: []byte("temp_blks_read")}, {Name: []byte("temp_blks_written")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testdb", Valid: true}, {String: "testuser", Valid: true}, {String: "SELECT test", Valid: true},
						{String: "1000", Valid: true}, {String: "2000", Valid: true},
						{String: "30000", Valid: true}, {String: "100", Valid: true}, {String: "6000", Valid: true}, {String: "4000", Valid: true}, {String: "100", Valid: true},
						{}, {}, {}, {}, {}, {}, {}, {}, {}, {},
					},
				},
			},
			want: map[string]postgresStatementStat{
				"testdb/testuser/c5ae4e2c19d733cda381b2dc24db57d7": {
					datname: "testdb", usename: "testuser", md5hash: "c5ae4e2c19d733cda381b2dc24db57d7", query: "SELECT test",
					calls: 1000, rows: 2000,
					totalExecTime: 30000, totalPlanTime: 100, blkReadTime: 6000, blkWriteTime: 4000,
					sharedBlksHit: 0, sharedBlksRead: 0, sharedBlksDirtied: 0, sharedBlksWritten: 0,
					localBlksHit: 0, localBlksRead: 0, localBlksDirtied: 0, localBlksWritten: 0,
					tempBlksRead: 0, tempBlksWritten: 0,
				},
			},
		},
		{
			// in this testcase, stats of first two rows should be grouped because of similar queries.
			name: "query normalization",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 19,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("datname")}, {Name: []byte("usename")}, {Name: []byte("query")},
					{Name: []byte("calls")}, {Name: []byte("rows")},
					{Name: []byte("total_exec_time")}, {Name: []byte("total_plan_time")}, {Name: []byte("blk_read_time")}, {Name: []byte("blk_write_time")},
					{Name: []byte("shared_blks_hit")}, {Name: []byte("shared_blks_read")}, {Name: []byte("shared_blks_dirtied")}, {Name: []byte("shared_blks_written")},
					{Name: []byte("local_blks_hit")}, {Name: []byte("local_blks_read")}, {Name: []byte("local_blks_dirtied")}, {Name: []byte("local_blks_written")},
					{Name: []byte("temp_blks_read")}, {Name: []byte("temp_blks_written")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testdb", Valid: true}, {String: "testuser", Valid: true}, {String: "SELECT 123", Valid: true},
						{String: "1000", Valid: true}, {String: "2000", Valid: true},
						{String: "30000", Valid: true}, {String: "100", Valid: true}, {String: "6000", Valid: true}, {String: "4000", Valid: true},
						{String: "100", Valid: true}, {String: "110", Valid: true}, {String: "120", Valid: true}, {String: "130", Valid: true},
						{String: "500", Valid: true}, {String: "510", Valid: true}, {String: "520", Valid: true}, {String: "530", Valid: true},
						{String: "700", Valid: true}, {String: "710", Valid: true},
					},
					{
						{String: "testdb", Valid: true}, {String: "testuser", Valid: true}, {String: "SELECT 456", Valid: true},
						{String: "1000", Valid: true}, {String: "2000", Valid: true},
						{String: "30000", Valid: true}, {String: "200", Valid: true}, {String: "6000", Valid: true}, {String: "4000", Valid: true},
						{String: "100", Valid: true}, {String: "110", Valid: true}, {String: "120", Valid: true}, {String: "130", Valid: true},
						{String: "500", Valid: true}, {String: "510", Valid: true}, {String: "520", Valid: true}, {String: "530", Valid: true},
						{String: "700", Valid: true}, {String: "710", Valid: true},
					},
					{
						{String: "testdb", Valid: true}, {String: "testuser", Valid: true}, {String: "SELECT 'whatever'", Valid: true},
						{String: "1000", Valid: true}, {String: "2000", Valid: true},
						{String: "30000", Valid: true}, {String: "300", Valid: true}, {String: "6000", Valid: true}, {String: "4000", Valid: true},
						{String: "100", Valid: true}, {String: "110", Valid: true}, {String: "120", Valid: true}, {String: "130", Valid: true},
						{String: "500", Valid: true}, {String: "510", Valid: true}, {String: "520", Valid: true}, {String: "530", Valid: true},
						{String: "700", Valid: true}, {String: "710", Valid: true},
					},
				},
			},
			want: map[string]postgresStatementStat{
				"testdb/testuser/095f2345f262d090a83ff1ac64ca8c76": {
					datname: "testdb", usename: "testuser", md5hash: "095f2345f262d090a83ff1ac64ca8c76", query: "SELECT ?",
					calls: 2000, rows: 4000,
					totalExecTime: 60000, totalPlanTime: 300, blkReadTime: 12000, blkWriteTime: 8000,
					sharedBlksHit: 200, sharedBlksRead: 220, sharedBlksDirtied: 240, sharedBlksWritten: 260,
					localBlksHit: 1000, localBlksRead: 1020, localBlksDirtied: 1040, localBlksWritten: 1060,
					tempBlksRead: 1400, tempBlksWritten: 1420,
				},
				"testdb/testuser/6fc7663c0674ba2b5e0239d56eddf235": {
					datname: "testdb", usename: "testuser", md5hash: "6fc7663c0674ba2b5e0239d56eddf235", query: "SELECT '?'",
					calls: 1000, rows: 2000,
					totalExecTime: 30000, totalPlanTime: 300, blkReadTime: 6000, blkWriteTime: 4000,
					sharedBlksHit: 100, sharedBlksRead: 110, sharedBlksDirtied: 120, sharedBlksWritten: 130,
					localBlksHit: 500, localBlksRead: 510, localBlksDirtied: 520, localBlksWritten: 530,
					tempBlksRead: 700, tempBlksWritten: 710,
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

func Test_normalizeStatement(t *testing.T) {
	f, err := os.Open("testdata/queries.golden")
	assert.NoError(t, err)

	scanner := bufio.NewScanner(f)

	var counter int
	var in, want string

	// Read file line by line, odd lines are input strings, even line are wanted strings. On even lines do test and assertion.
	for scanner.Scan() {
		counter++

		if counter%2 == 1 {
			in = scanner.Text()
		} else {
			want = scanner.Text()
		}

		if counter%2 == 0 {
			assert.Equal(t, want, normalizeStatement(in))
		}
	}

	// manual test
	in = ``
	want = ``
	assert.Equal(t, want, normalizeStatement(in))
}

func Test_selectStatementsQuery(t *testing.T) {
	testcases := []struct {
		version int
		want    string
	}{
		{version: PostgresV12, want: postgresStatementsQuery12},
		{version: PostgresV13, want: postgresStatementsQueryLatest},
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.want, selectStatementsQuery(tc.version))
	}
}
