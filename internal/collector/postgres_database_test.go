package collector

import (
	"database/sql"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/model"
	"testing"
)

func TestPostgresDatabasesCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_database_xact_commits_total",
			"postgres_database_xact_rollbacks_total",
			"postgres_database_blocks_total",
			"postgres_database_tuples_total",
			"postgres_database_temp_bytes_total",
			"postgres_database_temp_files_total",
			"postgres_database_conflicts_total",
			"postgres_database_deadlocks_total",
			"postgres_database_checksum_failures_total",
			"postgres_database_last_checksum_failure_seconds",
			"postgres_database_blk_time_seconds_total",
			"postgres_database_size_bytes",
			"postgres_database_stats_age_seconds_total",
			"postgres_xacts_left_before_wraparound",
		},
		// TODO: wait until Postgres 14 has been released, update Postgres version on pgscv-testing docker image
		//   and move these metrics to 'required' slice.
		optional: []string{
			"postgres_database_session_time_all_seconds_total",
			"postgres_database_session_time_seconds_total",
			"postgres_database_sessions_all_total",
			"postgres_database_sessions_total",
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
				Ncols: 27,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("database")},
					{Name: []byte("xact_commit")}, {Name: []byte("xact_rollback")}, {Name: []byte("blks_read")}, {Name: []byte("blks_hit")},
					{Name: []byte("tup_returned")}, {Name: []byte("tup_fetched")}, {Name: []byte("tup_inserted")}, {Name: []byte("tup_updated")}, {Name: []byte("tup_deleted")},
					{Name: []byte("conflicts")}, {Name: []byte("temp_files")}, {Name: []byte("temp_bytes")}, {Name: []byte("deadlocks")},
					{Name: []byte("checksum_failures")}, {Name: []byte("last_checksum_failure_unixtime")},
					{Name: []byte("blk_read_time")}, {Name: []byte("blk_write_time")},
					{Name: []byte("session_time")}, {Name: []byte("active_time")}, {Name: []byte("idle_in_transaction_time")},
					{Name: []byte("sessions")}, {Name: []byte("sessions_abandoned")}, {Name: []byte("sessions_fatal")}, {Name: []byte("sessions_killed")},
					{Name: []byte("size_bytes")}, {Name: []byte("stats_age_seconds")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testdb1", Valid: true},
						{String: "100", Valid: true}, {String: "5", Valid: true}, {String: "10000", Valid: true}, {String: "845785", Valid: true},
						{String: "758", Valid: true}, {String: "542", Valid: true}, {String: "452", Valid: true}, {String: "174", Valid: true}, {String: "125", Valid: true},
						{String: "33", Valid: true}, {String: "41", Valid: true}, {String: "85642585", Valid: true}, {String: "25", Valid: true},
						{String: "13", Valid: true}, {String: "1628668483", Valid: true},
						{String: "542542", Valid: true}, {String: "150150", Valid: true},
						{String: "12345678", Valid: true}, {String: "5425682", Valid: true}, {String: "125478", Valid: true},
						{String: "54872", Valid: true}, {String: "458", Valid: true}, {String: "8942", Valid: true}, {String: "69", Valid: true},
						{String: "485254752", Valid: true}, {String: "4589", Valid: true},
					},
					{
						{String: "testdb2", Valid: true},
						{String: "254", Valid: true}, {String: "41", Valid: true}, {String: "4853", Valid: true}, {String: "48752", Valid: true},
						{String: "7856", Valid: true}, {String: "4254", Valid: true}, {String: "894", Valid: true}, {String: "175", Valid: true}, {String: "245", Valid: true},
						{String: "26", Valid: true}, {String: "84", Valid: true}, {String: "125784686", Valid: true}, {String: "11", Valid: true},
						{String: "1", Valid: true}, {String: "54324565", Valid: true},
						{String: "458751", Valid: true}, {String: "235578", Valid: true},
						{String: "78541256", Valid: true}, {String: "8542214", Valid: true}, {String: "85475", Valid: true},
						{String: "854124", Valid: true}, {String: "8874", Valid: true}, {String: "4114", Valid: true}, {String: "5477", Valid: true},
						{String: "856964774", Valid: true}, {String: "6896", Valid: true},
					},
				},
			},
			want: map[string]postgresDatabaseStat{
				"testdb1": {
					database: "testdb1", xactcommit: 100, xactrollback: 5, blksread: 10000, blkshit: 845785,
					tupreturned: 758, tupfetched: 542, tupinserted: 452, tupupdated: 174, tupdeleted: 125,
					conflicts: 33, tempfiles: 41, tempbytes: 85642585, deadlocks: 25,
					csumfails: 13, csumlastfailunixts: 1628668483,
					blkreadtime: 542542, blkwritetime: 150150,
					sessiontime: 12345678, activetime: 5425682, idletxtime: 125478,
					sessions: 54872, sessabandoned: 458, sessfatal: 8942, sesskilled: 69,
					sizebytes: 485254752, statsage: 4589,
				},
				"testdb2": {
					database: "testdb2", xactcommit: 254, xactrollback: 41, blksread: 4853, blkshit: 48752,
					tupreturned: 7856, tupfetched: 4254, tupinserted: 894, tupupdated: 175, tupdeleted: 245,
					conflicts: 26, tempfiles: 84, tempbytes: 125784686, deadlocks: 11,
					csumfails: 1, csumlastfailunixts: 54324565,
					blkreadtime: 458751, blkwritetime: 235578,
					sessiontime: 78541256, activetime: 8542214, idletxtime: 85475,
					sessions: 854124, sessabandoned: 8874, sessfatal: 4114, sesskilled: 5477,
					sizebytes: 856964774, statsage: 6896,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresDatabasesStats(tc.res, []string{"database"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}

func Test_parsePostgresXidLimitStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want xidLimitStats
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows:    3,
				Ncols:    2,
				Colnames: []pgproto3.FieldDescription{{Name: []byte("src")}, {Name: []byte("to_limit")}},
				Rows: [][]sql.NullString{
					{{String: "database", Valid: true}, {String: "2145794333", Valid: true}},
					{{String: "prepared_xacts", Valid: true}, {String: "2147483647", Valid: true}},
					{{String: "replication_slots", Valid: true}, {String: "1845258812", Valid: true}},
					{{String: "invalid", Valid: true}, {String: "invalid", Valid: true}}, // this should be ignored, but logged
				},
			},
			want: xidLimitStats{database: 2145794333, prepared: 2147483647, replSlot: 1845258812},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresXidLimitStats(tc.res)
			assert.EqualValues(t, tc.want, got)
		})
	}
}

func Test_selectDatabasesQuery(t *testing.T) {
	testcases := []struct {
		version int
		want    string
	}{
		{version: PostgresV10, want: databasesQuery11},
		{version: PostgresV12, want: databasesQuery12},
		{version: PostgresV14, want: databasesQueryLatest},
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.want, selectDatabasesQuery(tc.version))
	}
}
