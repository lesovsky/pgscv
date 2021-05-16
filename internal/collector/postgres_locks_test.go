package collector

import (
	"database/sql"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/model"
	"testing"
)

func TestPostgresLocksCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_locks_in_flight",
			"postgres_locks_all_in_flight",
			"postgres_locks_not_granted_in_flight",
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
		want locksStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 10,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("access_share_lock")}, {Name: []byte("row_share_lock")},
					{Name: []byte("row_exclusive_lock")}, {Name: []byte("share_update_exclusive_lock")},
					{Name: []byte("share_lock")}, {Name: []byte("share_row_exclusive_lock")},
					{Name: []byte("exclusive_lock")}, {Name: []byte("access_exclusive_lock")},
					{Name: []byte("not_granted")}, {Name: []byte("total")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "11", Valid: true}, {String: "5", Valid: true},
						{String: "4", Valid: true}, {String: "8", Valid: true},
						{String: "7", Valid: true}, {String: "9", Valid: true},
						{String: "1", Valid: true}, {String: "2", Valid: true},
						{String: "6", Valid: true}, {String: "47", Valid: true},
					},
				},
			},
			want: locksStat{
				accessShareLock: 11, rowShareLock: 5, rowExclusiveLock: 4, shareUpdateExclusiveLock: 8,
				shareLock: 7, shareRowExclusiveLock: 9, exclusiveLock: 1, accessExclusiveLock: 2,
				notGranted: 6, total: 47,
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
