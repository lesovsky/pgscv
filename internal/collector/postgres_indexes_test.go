package collector

import (
	"database/sql"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostgresIndexesCollector_Update(t *testing.T) {
	var input = pipelineInput{
		optional: []string{
			"postgres_index_scans_total",
			"postgres_index_tuples_total",
			"postgres_index_io_blocks_total",
		},
		collector: NewPostgresIndexesCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresIndexStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]postgresIndexStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 9,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("datname")}, {Name: []byte("schemaname")}, {Name: []byte("relname")}, {Name: []byte("indexrelname")},
					{Name: []byte("idx_scan")}, {Name: []byte("idx_tup_read")}, {Name: []byte("idx_tup_fetch")},
					{Name: []byte("idx_blks_read")}, {Name: []byte("idx_blks_hit")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testdb", Valid: true}, {String: "testschema", Valid: true}, {String: "testrelname", Valid: true}, {String: "testindex", Valid: true},
						{String: "5842", Valid: true}, {String: "84572", Valid: true}, {String: "485", Valid: true}, {String: "4128", Valid: true}, {String: "847", Valid: true},
					},
				},
			},
			want: map[string]postgresIndexStat{
				"testdb/testschema/testrelname/testindex": {
					datname: "testdb", schemaname: "testschema", relname: "testrelname", indexname: "testindex",
					idxscan: 5842, idxtupread: 84572, idxtupfetch: 485, idxread: 4128, idxhit: 847,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresIndexStats(tc.res, []string{"datname", "schemaname", "relname", "indexrelname"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}
