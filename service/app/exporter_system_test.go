package app

import (
	"github.com/barcodepro/pgscv/service/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_collectSystemMetrics(t *testing.T) {
	s := model.TestSystemService()
	e, err := newExporter(s, NewServiceRepo())
	assert.NoError(t, err)
	assert.NotNil(t, e)

	var ch = make(chan prometheus.Metric)
	go func() {
		cnt := e.collectSystemMetrics(ch)
		close(ch)
		assert.Greater(t, cnt, 0)
	}()

	for i := range ch {
		assert.Contains(t, i.Desc().String(), "node_")
	}
}

func Test_collectCPUMetrics(t *testing.T) {
	s := model.TestSystemService()
	e, err := newExporter(s, NewServiceRepo())
	assert.NoError(t, err)
	assert.NotNil(t, e)

	var ch = make(chan prometheus.Metric)
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
	s := model.TestSystemService()
	e, err := newExporter(s, NewServiceRepo())
	assert.NoError(t, err)
	assert.NotNil(t, e)

	var ch = make(chan prometheus.Metric)
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
	s := model.TestSystemService()
	e, err := newExporter(s, NewServiceRepo())
	assert.NoError(t, err)
	assert.NotNil(t, e)

	var ch = make(chan prometheus.Metric)
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
	s := model.TestSystemService()
	e, err := newExporter(s, NewServiceRepo())
	assert.NoError(t, err)
	assert.NotNil(t, e)

	var ch = make(chan prometheus.Metric)
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
	s := model.TestSystemService()
	e, err := newExporter(s, NewServiceRepo())
	assert.NoError(t, err)
	assert.NotNil(t, e)

	var ch = make(chan prometheus.Metric)
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
	s := model.TestSystemService()
	e, err := newExporter(s, NewServiceRepo())
	assert.NoError(t, err)
	assert.NotNil(t, e)

	var ch = make(chan prometheus.Metric)
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
	s := model.TestSystemService()
	e, err := newExporter(s, NewServiceRepo())
	assert.NoError(t, err)
	assert.NotNil(t, e)

	var ch = make(chan prometheus.Metric)
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
	s := model.TestSystemService()
	e, err := newExporter(s, NewServiceRepo())
	assert.NoError(t, err)
	assert.NotNil(t, e)

	var ch = make(chan prometheus.Metric)
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
	s := model.TestSystemService()
	e, err := newExporter(s, NewServiceRepo())
	assert.NoError(t, err)
	assert.NotNil(t, e)

	var ch = make(chan prometheus.Metric)
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
	s := model.TestSystemService()
	e, err := newExporter(s, NewServiceRepo())
	assert.NoError(t, err)
	assert.NotNil(t, e)

	var ch = make(chan prometheus.Metric)
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
	s := model.TestSystemService()
	e, err := newExporter(s, NewServiceRepo())
	assert.NoError(t, err)
	assert.NotNil(t, e)

	var ch = make(chan prometheus.Metric)
	go func() {
		cnt := e.collectSystemUptime(ch)
		close(ch)
		assert.Greater(t, cnt, 0)
	}()

	for i := range ch {
		assert.Contains(t, i.Desc().String(), "node_uptime_seconds")
	}
}
