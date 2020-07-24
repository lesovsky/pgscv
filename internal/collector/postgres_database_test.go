package collector

import (
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"regexp"
	"testing"
)

func TestPostgresDatabasesCollector_Update(t *testing.T) {
	var requiredMetricNames = []string{
		"pgscv_database_xact_commit_total",
		"pgscv_database_xact_rollback_total",
	}

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
	}
}
