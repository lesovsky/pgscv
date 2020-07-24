package collector

import (
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"regexp"
	"testing"
)

// Important: this test might produce some warns because collector doesn't collect averages stored in stats.
func TestPgbouncerStatsCollector_Update(t *testing.T) {
	var requiredMetricNames = []string{
		"pgscv_pgbouncer_xact_total",
		"pgscv_pgbouncer_query_total",
		"pgscv_pgbouncer_received_bytes_total",
		"pgscv_pgbouncer_sent_bytes_total",
		"pgscv_pgbouncer_xact_time_seconds_total",
		"pgscv_pgbouncer_query_time_seconds_total",
		"pgscv_pgbouncer_wait_time_seconds_total",
	}

	collector, err := NewPgbouncerStatsCollector(prometheus.Labels{"example_label": "example_value"})
	assert.NoError(t, err)
	ch := make(chan prometheus.Metric)

	config := Config{ServiceType: model.ServiceTypePgbouncer, ConnString: "postgres://postgres@127.0.0.1:6432/pgbouncer"}

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
	}
}
