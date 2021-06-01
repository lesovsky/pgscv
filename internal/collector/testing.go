package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"regexp"
	"testing"
)

// pipelineInput
type pipelineInput struct {
	// Metrics names that must be generated during collector runtime. If any metric is not generated, pipeline fails.
	required []string
	// Metrics names that optionally should be generated during collector runtime. If some metric is not generated, pipeline
	// prints warning.
	optional []string
	// collector defines a function used for creating metric collector.
	collector func(labels, model.CollectorSettings) (Collector, error)
	// collectorSettings defines collector settings used during testing.
	collectorSettings model.CollectorSettings
	// Service type related to collector.
	service string
}

// Pipeline accepts input data (see pipelineInput), creates 'collector' and executes Update method for generating metrics.
// Generated metrics are catched and checked against passed slices of required/optional metrics.
// Pipeline fails in following cases 1) required metrics are not generated; 2) generated metrics are not present in required
// or optional slices
func pipeline(t *testing.T, input pipelineInput) {
	// requiredMetricNamesCounter is the counter of how many times metrics have been collected
	metricNamesCounter := map[string]int{}

	collector, err := input.collector(labels{"example_label": "example_value"}, input.collectorSettings)
	assert.NoError(t, err)
	ch := make(chan prometheus.Metric)

	var config Config
	switch input.service {
	case model.ServiceTypePostgresql:
		config.ConnString = "postgres://pgscv@127.0.0.1/postgres"
		cfg, err := NewPostgresServiceConfig(config.ConnString)
		assert.NoError(t, err)
		config.PostgresServiceConfig = cfg
	case model.ServiceTypePgbouncer:
		config.ConnString = "postgres://pgscv@127.0.0.1:6432/pgbouncer"
	}

	go func() {
		err := collector.Update(config, ch)
		assert.NoError(t, err)
		close(ch)
	}()

	// receive metrics from channel, extract name from the metric and check name of received metric exists in the test slice
	for metric := range ch {
		// skip nil values
		if metric == nil {
			continue
		}

		//log.Infoln("debug purpose: ", metric.Desc().String())
		re := regexp.MustCompile(`fqName: "([a-zA-Z0-9_]+)"`)
		match := re.FindStringSubmatch(metric.Desc().String())[1]
		assert.Contains(t, append(input.required, input.optional...), match)
		metricNamesCounter[match]++
	}

	for _, s := range input.required {
		if v, ok := metricNamesCounter[s]; !ok {
			assert.Fail(t, "necessary metric not found in the map: ", s)
		} else {
			assert.Greater(t, v, 0)
		}
	}

	// it'd be good if optional metrics counted, but not fail if they're not counted (old kernel?)
	for _, s := range input.optional {
		if v, ok := metricNamesCounter[s]; !ok {
			log.Warnf("optional metric not found in the map: %s, ", s)
		} else {
			assert.Greater(t, v, 0)
		}
	}
}
