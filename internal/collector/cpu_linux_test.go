package collector

import (
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"regexp"
	"testing"
)

func TestCPUCollector_Update(t *testing.T) {
	var requiredMetricNames = []string{
		"pgscv_cpu_seconds_total",
		"pgscv_cpu_guest_seconds_total",
	}

	collector, err := NewCPUCollector(prometheus.Labels{"example_label": "example_value"})
	assert.NoError(t, err)
	ch := make(chan prometheus.Metric)

	go func() {
		err := collector.Update(Config{ServiceType: model.ServiceTypeSystem}, ch)
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
