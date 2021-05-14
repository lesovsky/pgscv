package collector

import (
	"github.com/weaponry/pgscv/internal/model"
	"testing"
)

func TestPostgresBgwriterCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_ckpt_checkpoints_total",
			"postgres_ckpt_time_seconds_total",
			"postgres_ckpt_time_seconds_all_total",
			"postgres_written_bytes_total",
			"postgres_bgwriter_maxwritten_clean_total",
			"postgres_backends_fsync_total",
			"postgres_backends_allocated_bytes_total",
			"postgres_bgwriter_stats_age_seconds",
		},
		collector: NewPostgresBgwriterCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}
