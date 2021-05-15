package collector

import (
	"github.com/weaponry/pgscv/internal/model"
	"testing"
)

func TestPostgresLocksCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_locks_all_in_flight",
			"postgres_locks_in_flight",
			"postgres_locks_not_granted_in_flight",
		},
		collector: NewPostgresLocksCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}
