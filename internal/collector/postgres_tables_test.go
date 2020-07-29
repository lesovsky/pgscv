package collector

import (
	"database/sql"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostgresTablesCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_table_seq_scan_total",
			"postgres_table_seq_tup_read_total",
			"postgres_table_idx_scan_total",
			"postgres_table_idx_tup_fetch_total",
			"postgres_table_tuples_modified_total",
			"postgres_table_tuples_total",
			"postgres_table_last_vacuum_seconds",
			"postgres_table_last_analyze_seconds",
			"postgres_table_maintenance_total",
		},
		optional:  []string{},
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
				Ncols: 20,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("datname")}, {Name: []byte("schemaname")}, {Name: []byte("relname")},
					{Name: []byte("seq_scan")}, {Name: []byte("seq_tup_read")}, {Name: []byte("idx_scan")}, {Name: []byte("idx_tup_fetch")},
					{Name: []byte("n_tup_ins")}, {Name: []byte("n_tup_upd")}, {Name: []byte("n_tup_del")}, {Name: []byte("n_tup_hot_upd")},
					{Name: []byte("n_live_tup")}, {Name: []byte("n_dead_tup")}, {Name: []byte("n_mod_since_analyze")},
					{Name: []byte("last_vacuum_seconds")}, {Name: []byte("last_analyze_seconds")},
					{Name: []byte("vacuum_count")}, {Name: []byte("autovacuum_count")}, {Name: []byte("analyze_count")}, {Name: []byte("autoanalyze_count")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testdb", Valid: true}, {String: "testschema", Valid: true}, {String: "testrelname", Valid: true},
						{String: "100", Valid: true}, {String: "1000", Valid: true}, {String: "200", Valid: true}, {String: "2000", Valid: true},
						{String: "300", Valid: true}, {String: "400", Valid: true}, {String: "500", Valid: true}, {String: "150", Valid: true},
						{String: "600", Valid: true}, {String: "100", Valid: true}, {String: "500", Valid: true},
						{String: "700", Valid: true}, {String: "800", Valid: true},
						{String: "910", Valid: true}, {String: "920", Valid: true}, {String: "930", Valid: true}, {String: "940", Valid: true},
					},
				},
			},
			want: map[string]postgresTableStat{
				"testdb/testschema/testrelname": {
					datname: "testdb", schemaname: "testschema", relname: "testrelname",
					seqscan: 100, seqtupread: 1000, idxscan: 200, idxtupfetch: 2000,
					inserted: 300, updated: 400, deleted: 500, hotUpdated: 150, live: 600, dead: 100, modified: 500,
					lastvacuum: 700, lastanalyze: 800, vacuum: 910, autovacuum: 920, analyze: 930, autoanalyze: 940,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresTableStats(tc.res, []string{"datname", "schemaname", "relname"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}
