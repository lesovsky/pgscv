package collector

import (
	"database/sql"
	"github.com/jackc/pgproto3/v2"
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostgresBgwriterCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_checkpoints_total",
			"postgres_checkpoints_all_total",
			"postgres_checkpoints_seconds_total",
			"postgres_checkpoints_seconds_all_total",
			"postgres_written_bytes_total",
			"postgres_bgwriter_maxwritten_clean_total",
			"postgres_backends_fsync_total",
			"postgres_backends_allocated_bytes_total",
			"postgres_bgwriter_stats_age_seconds_total",
		},
		collector: NewPostgresBgwriterCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresBgwriterStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want postgresBgwriterStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 11,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("checkpoints_timed")}, {Name: []byte("checkpoints_req")},
					{Name: []byte("checkpoint_write_time")}, {Name: []byte("checkpoint_sync_time")},
					{Name: []byte("buffers_checkpoint")}, {Name: []byte("buffers_clean")}, {Name: []byte("maxwritten_clean")},
					{Name: []byte("buffers_backend")}, {Name: []byte("buffers_backend_fsync")}, {Name: []byte("buffers_alloc")},
					{Name: []byte("stats_age_seconds")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "55", Valid: true}, {String: "17", Valid: true},
						{String: "548425", Valid: true}, {String: "5425", Valid: true},
						{String: "5482", Valid: true}, {String: "7584", Valid: true}, {String: "452", Valid: true},
						{String: "6895", Valid: true}, {String: "2", Valid: true}, {String: "48752", Valid: true},
						{String: "5488", Valid: true},
					},
				},
			},
			want: postgresBgwriterStat{
				ckptTimed: 55, ckptReq: 17, ckptWriteTime: 548425, ckptSyncTime: 5425, ckptBuffers: 5482, bgwrBuffers: 7584, bgwrMaxWritten: 452,
				backendBuffers: 6895, backendFsync: 2, backendAllocated: 48752, statsAgeSeconds: 5488,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresBgwriterStats(tc.res)
			assert.EqualValues(t, tc.want, got)
		})
	}
}
