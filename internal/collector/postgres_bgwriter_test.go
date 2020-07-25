package collector

import (
	"github.com/barcodepro/pgscv/internal/model"
	"testing"
)

func TestPostgresBgwriterCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_bgwriter_ckpt_timed_total",
			"postgres_bgwriter_ckpt_req_total",
			"postgres_bgwriter_ckpt_write_time_seconds_total",
			"postgres_bgwriter_ckpt_sync_time_seconds_total",
			"postgres_bgwriter_buffers_written_total",
			"postgres_bgwriter_bgwr_maxwritten_clean_total",
			"postgres_bgwriter_backend_fsync_total",
			"postgres_bgwriter_backend_buffers_allocated_total",
			"postgres_bgwriter_stats_age_seconds",
		},
		collector: NewPostgresBgwriterCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}
