package collector

import (
	"database/sql"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/model"
	"testing"
)

func TestPgbouncerPoolsCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"pgbouncer_pool_connections_in_flight",
			"pgbouncer_pool_max_wait_seconds",
		},
		collector: NewPgbouncerPoolsCollector,
		service:   model.ServiceTypePgbouncer,
	}

	pipeline(t, input)
}

func Test_parsePgbouncerPoolsStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]pgbouncerPoolStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 2,
				Ncols: 11,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("database")}, {Name: []byte("user")},
					{Name: []byte("cl_active")}, {Name: []byte("cl_waiting")}, {Name: []byte("sv_active")}, {Name: []byte("sv_idle")},
					{Name: []byte("sv_used")}, {Name: []byte("sv_tested")}, {Name: []byte("sv_login")}, {Name: []byte("maxwait")},
					{Name: []byte("pool_mode")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testdb1", Valid: true}, {String: "testuser1", Valid: true},
						{String: "15", Valid: true}, {String: "5", Valid: true}, {String: "10", Valid: true}, {String: "1", Valid: true},
						{String: "1", Valid: true}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "1", Valid: true},
						{String: "transaction", Valid: true},
					},
					{
						{String: "testdb2", Valid: true}, {String: "testuser2", Valid: true},
						{String: "25", Valid: true}, {String: "10", Valid: true}, {String: "25", Valid: true}, {String: "2", Valid: true},
						{String: "2", Valid: true}, {String: "2", Valid: true}, {String: "2", Valid: true}, {String: "2", Valid: true},
						{String: "statement", Valid: true},
					},
				},
			},
			want: map[string]pgbouncerPoolStat{
				"testdb1/testuser1/transaction": {
					database: "testdb1", user: "testuser1", clActive: 15, clWaiting: 5, svActive: 10, svIdle: 1, svUsed: 1, svTested: 1, svLogin: 1, maxWait: 1, mode: "transaction",
				},
				"testdb2/testuser2/statement": {
					database: "testdb2", user: "testuser2", clActive: 25, clWaiting: 10, svActive: 25, svIdle: 2, svUsed: 2, svTested: 2, svLogin: 2, maxWait: 2, mode: "statement",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePgbouncerPoolsStats(tc.res, []string{"database", "user", "pool_mode"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}
