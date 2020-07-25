package collector

import (
	"github.com/barcodepro/pgscv/internal/model"
	"testing"
)

func TestPostgresTablesCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_table_seq_scan_total",
			"postgres_table_seq_tup_read_total",
		},
		optional:  []string{},
		collector: NewPostgresTablesCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}
