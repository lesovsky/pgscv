package collector

import (
	"github.com/barcodepro/pgscv/internal/model"
	"testing"
)

// Important: this test might produce some warns because collector doesn't collect averages stored in stats.
func TestPgbouncerStatsCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"pgbouncer_xact_total",
			"pgbouncer_query_total",
			"pgbouncer_received_bytes_total",
			"pgbouncer_sent_bytes_total",
			"pgbouncer_xact_time_seconds_total",
			"pgbouncer_query_time_seconds_total",
			"pgbouncer_wait_time_seconds_total",
		},
		collector: NewPgbouncerStatsCollector,
		service:   model.ServiceTypePgbouncer,
	}

	pipeline(t, input)
}
