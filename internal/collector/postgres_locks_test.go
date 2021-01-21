package collector

import (
	"database/sql"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostgresLocksCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_locks_in_flight",
		},
		collector: NewPostgresLocksCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresLocksStats(t *testing.T) {
	var testcases = []struct {
		name string
		res  *model.PGResult
		want map[string]float64
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows:    4,
				Ncols:    2,
				Colnames: []pgproto3.FieldDescription{{Name: []byte("mode")}, {Name: []byte("count")}},
				Rows: [][]sql.NullString{
					{{String: "RowExclusiveLock", Valid: true}, {String: "150", Valid: true}},
					{{String: "RowShareLock", Valid: true}, {String: "100", Valid: true}},
					{{String: "ExclusiveLock", Valid: true}, {String: "50", Valid: true}},
					{{String: "AccessShareLock", Valid: true}, {String: "2000", Valid: true}},
				},
			},
			want: map[string]float64{
				"RowExclusiveLock": 150,
				"RowShareLock":     100,
				"ExclusiveLock":    50,
				"AccessShareLock":  2000,
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresLocksStats(tc.res)
			assert.EqualValues(t, tc.want, got)
		})
	}
}
