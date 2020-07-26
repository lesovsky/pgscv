package collector

import (
	"github.com/barcodepro/pgscv/internal/model"
	"testing"
)

func TestPostgresActivityCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_activity_conn_total",
			"postgres_activity_prepared_xact_total",
		},
		collector: NewPostgresActivityCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}
