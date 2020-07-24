package collector

import (
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"regexp"
	"testing"
)

func TestPgbouncerPoolsCollector_Update(t *testing.T) {
	var requiredMetricNames = []string{
		"pgscv_pgbouncer_pool_cl_active_total",
		"pgscv_pgbouncer_pool_cl_waiting_total",
	}

	collector, err := NewPgbouncerPoolsCollector(prometheus.Labels{"example_label": "example_value"})
	assert.NoError(t, err)
	ch := make(chan prometheus.Metric)

	config := Config{ServiceType: model.ServiceTypePgbouncer, ConnString: "postgres://pgbouncer@127.0.0.1:6432/pgbouncer"}

	go func() {
		err := collector.Update(config, ch)
		assert.NoError(t, err)
		close(ch)
	}()

	// receive metrics from channel, extract name from the metric and check name of received metric exists in the test slice
	for metric := range ch {
		log.Infoln(metric.Desc().String())
		re := regexp.MustCompile(`fqName: "([a-z_]+)"`)
		match := re.FindStringSubmatch(metric.Desc().String())[1]
		assert.Contains(t, requiredMetricNames, match)
	}
}
