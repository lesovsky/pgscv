package collector

import (
	"database/sql"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostgresConflictsCollector_Update(t *testing.T) {
	var input = pipelineInput{
		optional: []string{
			"postgres_recovery_conflicts_total",
		},
		collector: NewPostgresConflictsCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresConflictsStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]postgresConflictStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 2,
				Ncols: 6,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("datname")}, {Name: []byte("confl_tablespace")}, {Name: []byte("confl_lock")},
					{Name: []byte("confl_snapshot")}, {Name: []byte("confl_bufferpin")}, {Name: []byte("confl_deadlock")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testdb1", Valid: true}, {String: "123", Valid: true}, {String: "548", Valid: true},
						{String: "784", Valid: true}, {String: "896", Valid: true}, {String: "896", Valid: true},
					},
					{
						{String: "testdb2", Valid: true}, {}, {}, {}, {}, {},
					},
				},
			},
			want: map[string]postgresConflictStat{
				"testdb1": {datname: "testdb1", tablespace: 123, lock: 548, snapshot: 784, bufferpin: 896, deadlock: 896},
				"testdb2": {datname: "testdb2"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresConflictStats(tc.res, []string{"datname", "reason"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}
