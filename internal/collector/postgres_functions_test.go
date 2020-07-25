package collector

import (
	"github.com/barcodepro/pgscv/internal/model"
	"testing"
)

/* IMPORTANT: this test will fail if there are no functions stats in the databases or track_functions is disabled */

func TestPostgresFunctionsCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_function_calls_total",
			"postgres_function_total_time_seconds",
			"postgres_function_self_time_seconds",
		},
		collector: NewPostgresFunctionsCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}
