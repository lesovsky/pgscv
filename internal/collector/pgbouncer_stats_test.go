package collector

import (
	"database/sql"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/model"
	"testing"
)

// Important: this test might produce some warns because collector doesn't collect averages stored in stats.
func TestPgbouncerStatsCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"pgbouncer_up",
			"pgbouncer_transactions_total",
			"pgbouncer_queries_total",
			"pgbouncer_bytes_total",
			"pgbouncer_spent_seconds_total",
		},
		collector: NewPgbouncerStatsCollector,
		service:   model.ServiceTypePgbouncer,
	}

	pipeline(t, input)
}

func Test_parsePgbouncerStatsStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]pgbouncerStatsStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 2,
				Ncols: 8,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("database")},
					{Name: []byte("total_xact_count")}, {Name: []byte("total_query_count")}, {Name: []byte("total_received")}, {Name: []byte("total_sent")},
					{Name: []byte("total_xact_time")}, {Name: []byte("total_query_time")}, {Name: []byte("total_wait_time")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testdb1", Valid: true},
						{String: "452789541", Valid: true}, {String: "45871254", Valid: true}, {String: "845758921", Valid: true}, {String: "584752366", Valid: true},
						{String: "854236758964", Valid: true}, {String: "489685327856", Valid: true}, {String: "865421752", Valid: true},
					},
					{
						{String: "testdb2", Valid: true},
						{String: "781245657", Valid: true}, {String: "45875233", Valid: true}, {String: "785452498", Valid: true}, {String: "587512688", Valid: true},
						{String: "786249684545", Valid: true}, {String: "871401521458", Valid: true}, {String: "4547111201", Valid: true},
					},
				},
			},
			want: map[string]pgbouncerStatsStat{
				"testdb1": {
					database: "testdb1", xacts: 452789541, queries: 45871254, received: 845758921, sent: 584752366, xacttime: 854236758964, querytime: 489685327856, waittime: 865421752,
				},
				"testdb2": {
					database: "testdb2", xacts: 781245657, queries: 45875233, received: 785452498, sent: 587512688, xacttime: 786249684545, querytime: 871401521458, waittime: 4547111201,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePgbouncerStatsStats(tc.res, []string{"database"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}
