package collector

import (
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"regexp"
	"testing"
)

func TestPostgresBgwriterCollector_Update(t *testing.T) {
	var (
		// requiredMetricNames contains all metrics we want to see in the output
		requiredMetricNames = []string{
			"pgscv_postgres_checkpoints_timed_total",
			"pgscv_postgres_checkpoints_req_total",
			"pgscv_postgres_write_time_seconds_total",
			"pgscv_postgres_sync_time_seconds_total",
			"pgscv_postgres_buffers_written_total",
			"pgscv_postgres_maxwritten_clean_total",
			"pgscv_postgres_backend_fsync_total",
			"pgscv_postgres_buffers_allocated_total",
			"pgscv_postgres_bgwriter_stats_age_seconds",
		}

		// requiredMetricNamesCounter is the counter of how many times metrics have been collected
		requiredMetricNamesCounter = map[string]int{}
	)

	collector, err := NewPostgresBgwriterCollector(prometheus.Labels{"example_label": "example_value"})
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
