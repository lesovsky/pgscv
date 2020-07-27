package collector

import (
	"github.com/barcodepro/pgscv/internal/model"
	"testing"
)

func TestPostgresStatementsCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_statements_calls",
		},
		optional:  []string{},
		collector: NewPostgresStatementsCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}
