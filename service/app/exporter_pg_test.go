package app

import (
	"github.com/barcodepro/pgscv/service/internal/log"
	"github.com/barcodepro/pgscv/service/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func Test_collectPostgresMetrics(t *testing.T) {
	log.SetLevel("warn")
	s := model.TestPostgresService()
	e, err := newExporter(s, NewServiceRepo())
	assert.NoError(t, err)
	assert.NotNil(t, e)

	var ch = make(chan prometheus.Metric)
	go func() {
		cnt := e.collectPostgresMetrics(ch, s)
		close(ch)
		assert.Greater(t, cnt, 0)
	}()

	var m = make(map[string]int)
	for i := range ch {
		assert.Contains(t, i.Desc().String(), "pg_")
		ss := strings.Split(i.Desc().String(), " ")
		key := strings.TrimRight(strings.TrimLeft(ss[1], `"`), `",`)
		m[key]++
	}

	var total, absent float64
	for k := range globalHelpCatalog() {
		if !strings.HasPrefix(k, "pg_") {
			continue
		}
		total++
		if _, ok := m[k]; !ok {
			t.Logf("absent %s", k)
			absent++
		}
	}
	pct := 100 * absent / total
	t.Logf("metrics: total %.0f, absent %.0f, absent %.2f%%\n", total, absent, pct)
	assert.Less(t, pct, absentMetricsThreshold)
}

func Test_collectPgbouncerMetrics(t *testing.T) {
	log.SetLevel("warn")
	s := model.TestPgbouncerService()
	e, err := newExporter(s, NewServiceRepo())
	assert.NoError(t, err)
	assert.NotNil(t, e)

	var ch = make(chan prometheus.Metric)
	go func() {
		cnt := e.collectPgbouncerMetrics(ch, s)
		close(ch)
		assert.Greater(t, cnt, 0)
	}()

	var m = make(map[string]int)
	for i := range ch {
		assert.Contains(t, i.Desc().String(), "pgbouncer_")
		ss := strings.Split(i.Desc().String(), " ")
		key := strings.TrimRight(strings.TrimLeft(ss[1], `"`), `",`)
		m[key]++
	}

	var total, absent float64
	for k := range globalHelpCatalog() {
		if !strings.HasPrefix(k, "pgbouncer_") {
			continue
		}
		total++
		if _, ok := m[k]; !ok {
			t.Logf("absent %s", k)
			absent++
		}
	}
	pct := 100 * absent / total
	t.Logf("metrics: total %.0f, absent %.0f, absent %.2f%%\n", total, absent, pct)
	assert.Less(t, pct, absentMetricsThreshold)
}
