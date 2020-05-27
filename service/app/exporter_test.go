package app

import (
	"github.com/barcodepro/pgscv/service/internal/log"
	"github.com/barcodepro/pgscv/service/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_NewExporter(t *testing.T) {
	r := NewServiceRepo()
	e, err := newExporter(model.TestSystemService(), r)
	assert.NoError(t, err)
	assert.NotNil(t, e)
	assert.Equal(t, "system", e.ServiceID)
	assert.NotNil(t, e.AllDesc)
	assert.NotNil(t, e.ServiceRepo)
}

func TestPrometheusExporter_Describe(t *testing.T) {
	var testCases = []struct {
		service  model.Service
		expected string
	}{
		{service: model.TestSystemService(), expected: "node_"},
		{service: model.TestPostgresService(), expected: "pg_"},
		{service: model.TestPgbouncerService(), expected: "pgbouncer_"},
	}

	for _, tc := range testCases {
		e, err := newExporter(tc.service, NewServiceRepo())
		assert.NoError(t, err)
		assert.NotNil(t, e)

		var ch = make(chan *prometheus.Desc)
		go func() {
			e.Describe(ch)
			close(ch)
		}()

		for i := range ch {
			assert.Contains(t, i.String(), tc.expected)
		}
	}
}

func TestPrometheusExporter_Collect(t *testing.T) {
	var testCases = []struct {
		service  model.Service
		expected string
	}{
		{service: model.TestSystemService(), expected: "node_"},
		{service: model.TestPostgresService(), expected: "pg_"},
		{service: model.TestPgbouncerService(), expected: "pgbouncer_"},
	}

	for _, tc := range testCases {
		log.SetLevel("warn")
		r := NewServiceRepo()
		r.addService(tc.service.ServiceID, tc.service)

		e, err := newExporter(tc.service, r)
		assert.NoError(t, err)
		assert.NotNil(t, e)

		var ch = make(chan prometheus.Metric)
		go func() {
			e.Collect(ch)
			close(ch)
		}()

		var cnt int
		for i := range ch {
			cnt++
			assert.Contains(t, i.Desc().String(), tc.expected)
		}
		assert.Greater(t, cnt, 0)
	}

	// special case - repeating connection to broken service and finally removing it from the repo
	r := NewServiceRepo()
	s := model.TestPostgresService()
	s.ConnSettings.Conninfo = "host=127.0.0.1 port=15432 user=invalid dbname=invalid"
	r.addService(s.ServiceID, s)

	e, err := newExporter(s, r)
	assert.NoError(t, err)
	assert.NotNil(t, e)

	for i := 0; i < exporterFailureLimit; i++ {
		var ch = make(chan prometheus.Metric)
		go func() {
			e.Collect(ch)
			close(ch)
		}()

		var cnt int
		for range ch {
			cnt++
		}
		assert.Equal(t, 0, cnt)
	}
	assert.Equal(t, 0, r.totalServices()) // service repo should not contain services
}

func Test_Contains(t *testing.T) {
	ss := []string{"first_example_string", "second_example_string", "third_example_string"}

	assert.True(t, stringsContains(ss, "first_example_string"))
	assert.False(t, stringsContains(ss, "unknown_string"))
}
