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
			"postgres_table_idx_scan_total",
			"postgres_table_idx_tup_fetch_total",
			"postgres_table_tuples_modified_total",
			"postgres_table_tuples_total",
			"postgres_table_last_vacuum_seconds",
			"postgres_table_last_analyze_seconds",
			"postgres_table_maintenance_total",
		},
		optional:  []string{},
		collector: NewPostgresTablesCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}
