package app

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"pgscv/app/model"
	"strings"
	"testing"
)

func testSystemService() model.Service {
	return model.Service{
		ServiceType: model.ServiceTypeSystem,
		ServiceID:   "system",
	}
}

func testPostgresService() model.Service {
	return model.Service{
		ServiceType: model.ServiceTypePostgresql,
		ServiceID:   "postgres:5432",
		Host:        "127.0.0.1",
		Port:        5432,
		User:        "weaponry_app",
		Dbname:      "postgres",
	}
}

func testPgbouncerService() model.Service {
	return model.Service{
		ServiceType: model.ServiceTypePgbouncer,
		ServiceID:   "pgbouncer:6432",
		Host:        "127.0.0.1",
		Port:        6432,
		User:        "weaponry_app",
		Dbname:      "pgbouncer",
	}
}

func Test_collectCPUMetrics(t *testing.T) {
	var service = model.Service{ServiceType: model.ServiceTypeSystem, ServiceID: "system"}
	var ch = make(chan prometheus.Metric)

	e, err := newExporter(service, &ServiceRepo{})
	assert.NoError(t, err)
	assert.NotNil(t, e)

	go func() {
		cnt := e.collectCPUMetrics(ch)
		close(ch)
		assert.Greater(t, cnt, 0)
	}()

	for i := range ch {
		assert.Contains(t, i.Desc().String(), "node_cpu_usage_time")
	}
}

func Test_collectMemMetrics(t *testing.T) {
	var service = model.Service{ServiceType: model.ServiceTypeSystem, ServiceID: "system"}
	var ch = make(chan prometheus.Metric)

	e, err := newExporter(service, &ServiceRepo{})
	assert.NoError(t, err)
	assert.NotNil(t, e)

	go func() {
		cnt := e.collectMemMetrics(ch)
		close(ch)
		assert.Greater(t, cnt, 0)
	}()

	for i := range ch {
		assert.Contains(t, i.Desc().String(), "node_memory_usage_bytes")
	}
}

func Test_collectDiskstatsMetrics(t *testing.T) {
	var service = model.Service{ServiceType: model.ServiceTypeSystem, ServiceID: "system"}
	var ch = make(chan prometheus.Metric)

	e, err := newExporter(service, &ServiceRepo{})
	assert.NoError(t, err)
	assert.NotNil(t, e)

	go func() {
		cnt := e.collectDiskstatsMetrics(ch)
		close(ch)
		assert.Greater(t, cnt, 0)
	}()

	for i := range ch {
		assert.Contains(t, i.Desc().String(), "node_diskstats_")
	}
}

func Test_collectNetdevMetrics(t *testing.T) {
	var service = model.Service{ServiceType: model.ServiceTypeSystem, ServiceID: "system"}
	var ch = make(chan prometheus.Metric)

	e, err := newExporter(service, &ServiceRepo{})
	assert.NoError(t, err)
	assert.NotNil(t, e)

	go func() {
		cnt := e.collectNetdevMetrics(ch)
		close(ch)
		assert.Greater(t, cnt, 0)
	}()

	for i := range ch {
		assert.Contains(t, i.Desc().String(), "node_netdev_")
	}
}

func Test_collectFsMetrics(t *testing.T) {
	var service = model.Service{ServiceType: model.ServiceTypeSystem, ServiceID: "system"}
	var ch = make(chan prometheus.Metric)

	e, err := newExporter(service, &ServiceRepo{})
	assert.NoError(t, err)
	assert.NotNil(t, e)

	go func() {
		cnt := e.collectFsMetrics(ch)
		close(ch)
		assert.Greater(t, cnt, 0)
	}()

	for i := range ch {
		assert.Contains(t, i.Desc().String(), "node_filesystem_")
	}
}

func Test_collectSysctlMetrics(t *testing.T) {
	var service = model.Service{ServiceType: model.ServiceTypeSystem, ServiceID: "system"}
	var ch = make(chan prometheus.Metric)

	e, err := newExporter(service, &ServiceRepo{})
	assert.NoError(t, err)
	assert.NotNil(t, e)

	go func() {
		cnt := e.collectSysctlMetrics(ch)
		close(ch)
		assert.Greater(t, cnt, 0)
	}()

	for i := range ch {
		assert.Contains(t, i.Desc().String(), "node_settings_sysctl")
	}
}

func Test_collectCPUCoresState(t *testing.T) {
	var service = model.Service{ServiceType: model.ServiceTypeSystem, ServiceID: "system"}
	var ch = make(chan prometheus.Metric)

	e, err := newExporter(service, &ServiceRepo{})
	assert.NoError(t, err)
	assert.NotNil(t, e)

	go func() {
		cnt := e.collectCPUCoresState(ch)
		close(ch)
		assert.Greater(t, cnt, 0)
	}()

	for i := range ch {
		assert.Contains(t, i.Desc().String(), "node_hardware_cores_total")
	}
}

func Test_collectCPUScalingGovernors(t *testing.T) {
	var service = model.Service{ServiceType: model.ServiceTypeSystem, ServiceID: "system"}
	var ch = make(chan prometheus.Metric)

	e, err := newExporter(service, &ServiceRepo{})
	assert.NoError(t, err)
	assert.NotNil(t, e)

	go func() {
		cnt := e.collectCPUScalingGovernors(ch)
		close(ch)
		assert.Greater(t, cnt, 0)
	}()

	for i := range ch {
		assert.Contains(t, i.Desc().String(), "node_hardware_scaling_governors")
	}
}

func Test_collectNumaNodes(t *testing.T) {
	var service = model.Service{ServiceType: model.ServiceTypeSystem, ServiceID: "system"}
	var ch = make(chan prometheus.Metric)

	e, err := newExporter(service, &ServiceRepo{})
	assert.NoError(t, err)
	assert.NotNil(t, e)

	go func() {
		cnt := e.collectNumaNodes(ch)
		close(ch)
		assert.Greater(t, cnt, 0)
	}()

	for i := range ch {
		assert.Contains(t, i.Desc().String(), "node_hardware_numa_nodes")
	}
}

func Test_collectStorageSchedulers(t *testing.T) {
	var service = model.Service{ServiceType: model.ServiceTypeSystem, ServiceID: "system"}
	var ch = make(chan prometheus.Metric)

	e, err := newExporter(service, &ServiceRepo{})
	assert.NoError(t, err)
	assert.NotNil(t, e)

	go func() {
		cnt := e.collectStorageSchedulers(ch)
		close(ch)
		assert.Greater(t, cnt, 0)
	}()

	for i := range ch {
		assert.Contains(t, i.Desc().String(), "node_hardware_storage_rotational")
	}
}

func Test_collectSystemUptime(t *testing.T) {
	var service = testSystemService()
	var ch = make(chan prometheus.Metric)

	e, err := newExporter(service, &ServiceRepo{})
	assert.NoError(t, err)
	assert.NotNil(t, e)

	go func() {
		cnt := e.collectSystemUptime(ch)
		close(ch)
		assert.Greater(t, cnt, 0)
	}()

	for i := range ch {
		assert.Contains(t, i.Desc().String(), "node_uptime_seconds")
	}
}

func Test_collectPgMetrics_postgres(t *testing.T) {
	var service = testPostgresService()
	var ch = make(chan prometheus.Metric)

	e, err := newExporter(service, &ServiceRepo{})
	assert.NoError(t, err)
	assert.NotNil(t, e)

	go func() {
		cnt := e.collectPgMetrics(ch, service)
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

func Test_collectPgMetrics_pgbouncer(t *testing.T) {
	var service = testPgbouncerService()
	var ch = make(chan prometheus.Metric)

	e, err := newExporter(service, &ServiceRepo{})
	assert.NoError(t, err)
	assert.NotNil(t, e)

	go func() {
		cnt := e.collectPgMetrics(ch, service)
		close(ch)
		assert.Greater(t, cnt, 0)
	}()

	for i := range ch {
		assert.Contains(t, i.Desc().String(), "pgbouncer_")
	}
}
