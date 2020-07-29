package collector

import (
	"database/sql"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostgresStorageCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_temp_files_in_flight",
			"postgres_temp_bytes_in_flight",
			"postgres_temp_files_max_age_seconds",
		},
		collector: NewPostgresStorageCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresTempFileInflght(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]postgresTempfilesStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 4,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("tablespace")}, {Name: []byte("files_total")}, {Name: []byte("bytes_total")}, {Name: []byte("max_age_seconds")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testtablespace", Valid: true}, {String: "45", Valid: true}, {String: "84523654741", Valid: true}, {String: "578", Valid: true},
					},
				},
			},
			want: map[string]postgresTempfilesStat{
				"testtablespace": {tablespace: "testtablespace", tempfiles: 45, tempbytes: 84523654741, tempmaxage: 578},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresTempFileInflght(tc.res)
			assert.EqualValues(t, tc.want, got)
		})
	}
}
