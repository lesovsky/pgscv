package collector

import (
	"database/sql"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/model"
	"testing"
)

func TestPostgresTablesCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_table_seq_scan_total",
			"postgres_table_seq_tup_read_total",
			"postgres_table_idx_scan_total",
			"postgres_table_idx_tup_fetch_total",
			"postgres_table_tuples_inserted_total",
			"postgres_table_tuples_updated_total",
			"postgres_table_tuples_hot_updated_total",
			"postgres_table_tuples_deleted_total",
			"postgres_table_tuples_live_total",
			"postgres_table_tuples_dead_total",
			"postgres_table_tuples_modified_total",
			"postgres_table_since_last_vacuum_seconds_total",
			"postgres_table_since_last_analyze_seconds_total",
			"postgres_table_last_vacuum_time",
			"postgres_table_last_analyze_time",
			"postgres_table_maintenance_total",
			"postgres_table_size_bytes",
			"postgres_table_tuples_total",
		},
		optional: []string{
			"postgres_table_io_blocks_total",
		},
		collector: NewPostgresTablesCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresTableStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]postgresTableStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 32,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("database")}, {Name: []byte("schema")}, {Name: []byte("table")},
					{Name: []byte("seq_scan")}, {Name: []byte("seq_tup_read")}, {Name: []byte("idx_scan")}, {Name: []byte("idx_tup_fetch")},
					{Name: []byte("n_tup_ins")}, {Name: []byte("n_tup_upd")}, {Name: []byte("n_tup_del")}, {Name: []byte("n_tup_hot_upd")},
					{Name: []byte("n_live_tup")}, {Name: []byte("n_dead_tup")}, {Name: []byte("n_mod_since_analyze")},
					{Name: []byte("last_vacuum_seconds")}, {Name: []byte("last_analyze_seconds")}, {Name: []byte("last_vacuum_time")}, {Name: []byte("last_analyze_time")},
					{Name: []byte("vacuum_count")}, {Name: []byte("autovacuum_count")}, {Name: []byte("analyze_count")}, {Name: []byte("autoanalyze_count")},
					{Name: []byte("heap_blks_read")}, {Name: []byte("heap_blks_hit")}, {Name: []byte("idx_blks_read")}, {Name: []byte("idx_blks_hit")},
					{Name: []byte("toast_blks_read")}, {Name: []byte("toast_blks_hit")}, {Name: []byte("tidx_blks_read")}, {Name: []byte("tidx_blks_hit")},
					{Name: []byte("size_bytes")}, {Name: []byte("reltuples")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testdb", Valid: true}, {String: "testschema", Valid: true}, {String: "testrelname", Valid: true},
						{String: "100", Valid: true}, {String: "1000", Valid: true}, {String: "200", Valid: true}, {String: "2000", Valid: true},
						{String: "300", Valid: true}, {String: "400", Valid: true}, {String: "500", Valid: true}, {String: "150", Valid: true},
						{String: "600", Valid: true}, {String: "100", Valid: true}, {String: "500", Valid: true},
						{String: "700", Valid: true}, {String: "800", Valid: true}, {String: "12345678", Valid: true}, {String: "87654321", Valid: true},
						{String: "910", Valid: true}, {String: "920", Valid: true}, {String: "930", Valid: true}, {String: "940", Valid: true},
						{String: "4528", Valid: true}, {String: "5845", Valid: true}, {String: "458", Valid: true}, {String: "698", Valid: true},
						{String: "125", Valid: true}, {String: "825", Valid: true}, {String: "699", Valid: true}, {String: "375", Valid: true},
						{String: "458523", Valid: true}, {String: "50000", Valid: true},
					},
				},
			},
			want: map[string]postgresTableStat{
				"testdb/testschema/testrelname": {
					database: "testdb", schema: "testschema", table: "testrelname",
					seqscan: 100, seqtupread: 1000, idxscan: 200, idxtupfetch: 2000,
					inserted: 300, updated: 400, deleted: 500, hotUpdated: 150, live: 600, dead: 100, modified: 500,
					lastvacuumAge: 700, lastanalyzeAge: 800, lastvacuumTime: 12345678, lastanalyzeTime: 87654321, vacuum: 910, autovacuum: 920, analyze: 930, autoanalyze: 940,
					heapread: 4528, heaphit: 5845, idxread: 458, idxhit: 698, toastread: 125, toasthit: 825, tidxread: 699, tidxhit: 375,
					sizebytes: 458523, reltuples: 50000,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresTableStats(tc.res, []string{"database", "schema", "table"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}
