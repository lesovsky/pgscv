package collector

import (
	"github.com/barcodepro/pgscv/internal/model"
	"testing"
)

func TestPgbouncerPoolsCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"pgbouncer_pool_conn_total",
			"pgbouncer_pool_max_wait_seconds",
		},
		collector: NewPgbouncerPoolsCollector,
		service:   model.ServiceTypePgbouncer,
	}

	pipeline(t, input)
}
