package collector

import (
	"github.com/weaponry/pgscv/internal/model"
	"testing"
)

func TestPostgresWalArchivingCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{},
		optional: []string{
			"postgres_archiver_archived_total",
			"postgres_archiver_failed_total",
			"postgres_archiver_since_last_archive_seconds",
			"postgres_archiver_lag_bytes",
		},
		collector: NewPostgresWalArchivingCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}
