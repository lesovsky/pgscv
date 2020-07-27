package collector

import (
	"github.com/barcodepro/pgscv/internal/model"
	"testing"
)

func TestPostgresStatementsCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_statements_calls_total",
			"postgres_statements_rows_total",
			"postgres_statements_time_total",
			"postgres_statements_blocks_total",
		},
		optional:  []string{},
		collector: NewPostgresStatementsCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}
