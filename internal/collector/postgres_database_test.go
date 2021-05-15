package collector

import (
	"github.com/weaponry/pgscv/internal/model"
	"testing"
)

func TestPostgresDatabasesCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_database_xact_commits_total",
			"postgres_database_xact_rollbacks_total",
			"postgres_database_conflicts_total",
			"postgres_database_deadlocks_total",
			"postgres_database_blocks_total",
			"postgres_database_tuples_total",
			"postgres_database_temp_bytes_total",
			"postgres_database_temp_files_total",
			"postgres_database_blk_time_seconds",
			"postgres_database_size_bytes",
			"postgres_database_stats_age_seconds",
			"postgres_xacts_left_before_wraparound",
		},
		collector: NewPostgresDatabasesCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}
