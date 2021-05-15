package collector

import (
	"github.com/weaponry/pgscv/internal/model"
	"testing"
)

func TestPostgresIndexesCollector_Update(t *testing.T) {
	var input = pipelineInput{
		optional: []string{
			"postgres_index_scans_total",
			"postgres_index_tuples_total",
			"postgres_index_io_blocks_total",
			"postgres_index_size_bytes",
		},
		collector: NewPostgresIndexesCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}
