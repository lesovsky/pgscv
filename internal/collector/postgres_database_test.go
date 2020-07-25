package collector

import (
	"github.com/barcodepro/pgscv/internal/model"
	"testing"
)

func TestPostgresDatabasesCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_database_xact_commit_total",
			"postgres_database_xact_rollback_total",
			"postgres_database_blks_read_total",
			"postgres_database_blks_hit_total",
			"postgres_database_tup_returned_total",
			"postgres_database_tup_fetched_total",
			"postgres_database_tup_inserted_total",
			"postgres_database_tup_updated_total",
			"postgres_database_tup_deleted_total",
			"postgres_database_conflicts_total",
			"postgres_database_temp_files_total",
			"postgres_database_temp_bytes_total",
			"postgres_database_deadlocks_total",
			"postgres_database_blk_read_time_seconds",
			"postgres_database_blk_write_time_seconds",
			"postgres_database_size_bytes_total",
			"postgres_database_stats_age_seconds",
		},
		collector: NewPostgresDatabasesCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}
