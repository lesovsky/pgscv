package collector

import (
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"regexp"
	"testing"
)

func TestPostgresDatabasesCollector_Update(t *testing.T) {
	var (
		// requiredMetricNames contains all metrics we want to see in the output
		requiredMetricNames = []string{
			"pgscv_database_xact_commit_total",
			"pgscv_database_xact_rollback_total",
			"pgscv_database_blks_read_total",
			"pgscv_database_blks_hit_total",
			"pgscv_database_tup_returned_total",
			"pgscv_database_tup_fetched_total",
			"pgscv_database_tup_inserted_total",
			"pgscv_database_tup_updated_total",
			"pgscv_database_tup_deleted_total",
			"pgscv_database_conflicts_total",
			"pgscv_database_temp_files_total",
			"pgscv_database_temp_bytes_total",
			"pgscv_database_deadlocks_total",
			"pgscv_database_blk_read_time_seconds",
			"pgscv_database_blk_write_time_seconds",
			"pgscv_database_size_bytes_total",
			"pgscv_database_stats_age_seconds",
		}

		// requiredMetricNamesCounter is the counter of how many times metric have been collected
		requiredMetricNamesCounter = map[string]int{}
	)

	collector, err := NewPostgresDatabasesCollector(prometheus.Labels{"example_label": "example_value"})
	assert.NoError(t, err)
	ch := make(chan prometheus.Metric)

	config := Config{ServiceType: model.ServiceTypePostgresql, ConnString: "postgres://postgres@postgres/postgres"}

	go func() {
		err := collector.Update(config, ch)
		assert.NoError(t, err)
		close(ch)
	}()

	// receive metrics from channel, extract name from the metric and check name of received metric exists in the test slice
	for metric := range ch {
		re := regexp.MustCompile(`fqName: "([a-z_]+)"`)
		match := re.FindStringSubmatch(metric.Desc().String())[1]
		assert.Contains(t, requiredMetricNames, match)
		requiredMetricNamesCounter[match] += 1
	}

	for _, s := range requiredMetricNames {
		if v, ok := requiredMetricNamesCounter[s]; !ok {
			assert.Fail(t, "necessary metric not found in the map: ", s)
		} else {
			assert.Greater(t, v, 0)
		}
	}
}
