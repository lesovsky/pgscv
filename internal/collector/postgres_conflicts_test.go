package collector

import (
	"github.com/weaponry/pgscv/internal/model"
	"testing"
)

func TestPostgresConflictsCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_recovery_conflicts_total",
		},
		collector: NewPostgresConflictsCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}
