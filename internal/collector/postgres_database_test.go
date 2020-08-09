package collector

import (
	"database/sql"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostgresDatabasesCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_database_events_total",
			"postgres_database_blocks_total",
			"postgres_database_tuples_total",
			"postgres_database_temp_bytes_total",
			"postgres_database_blk_time_seconds",
			"postgres_database_size_bytes_total",
			"postgres_database_stats_age_seconds",
		},
		collector: NewPostgresDatabasesCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresDatabasesStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]postgresDatabaseStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 2,
				Ncols: 18,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("datname")},
					{Name: []byte("xact_commit")}, {Name: []byte("xact_rollback")}, {Name: []byte("blks_read")}, {Name: []byte("blks_hit")},
					{Name: []byte("tup_returned")}, {Name: []byte("tup_fetched")}, {Name: []byte("tup_inserted")}, {Name: []byte("tup_updated")}, {Name: []byte("tup_deleted")},
					{Name: []byte("conflicts")}, {Name: []byte("temp_files")}, {Name: []byte("temp_bytes")}, {Name: []byte("deadlocks")},
					{Name: []byte("blk_read_time")}, {Name: []byte("blk_write_time")}, {Name: []byte("size_bytes")}, {Name: []byte("stats_age_seconds")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testdb1", Valid: true},
						{String: "100", Valid: true}, {String: "5", Valid: true}, {String: "10000", Valid: true}, {String: "845785", Valid: true},
						{String: "758", Valid: true}, {String: "542", Valid: true}, {String: "452", Valid: true}, {String: "174", Valid: true}, {String: "125", Valid: true},
						{String: "33", Valid: true}, {String: "41", Valid: true}, {String: "85642585", Valid: true}, {String: "25", Valid: true},
						{String: "542542", Valid: true}, {String: "150150", Valid: true}, {String: "485254752", Valid: true}, {String: "4589", Valid: true},
					},
					{
						{String: "testdb2", Valid: true},
						{String: "254", Valid: true}, {String: "41", Valid: true}, {String: "4853", Valid: true}, {String: "48752", Valid: true},
						{String: "7856", Valid: true}, {String: "4254", Valid: true}, {String: "894", Valid: true}, {String: "175", Valid: true}, {String: "245", Valid: true},
						{String: "26", Valid: true}, {String: "84", Valid: true}, {String: "125784686", Valid: true}, {String: "11", Valid: true},
						{String: "458751", Valid: true}, {String: "235578", Valid: true}, {String: "856964774", Valid: true}, {String: "6896", Valid: true},
					},
				},
			},
			want: map[string]postgresDatabaseStat{
				"testdb1": {
					datname: "testdb1", xactcommit: 100, xactrollback: 5, blksread: 10000, blkshit: 845785,
					tupreturned: 758, tupfetched: 542, tupinserted: 452, tupupdated: 174, tupdeleted: 125,
					conflicts: 33, tempfiles: 41, tempbytes: 85642585, deadlocks: 25,
					blkreadtime: 542542, blkwritetime: 150150, sizebytes: 485254752, statsage: 4589,
				},
				"testdb2": {
					datname: "testdb2", xactcommit: 254, xactrollback: 41, blksread: 4853, blkshit: 48752,
					tupreturned: 7856, tupfetched: 4254, tupinserted: 894, tupupdated: 175, tupdeleted: 245,
					conflicts: 26, tempfiles: 84, tempbytes: 125784686, deadlocks: 11,
					blkreadtime: 458751, blkwritetime: 235578, sizebytes: 856964774, statsage: 6896,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresDatabasesStats(tc.res, []string{"datname"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}
