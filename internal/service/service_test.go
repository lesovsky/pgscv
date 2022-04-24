package service

import (
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRepository_addService(t *testing.T) {
	r := NewRepository()
	s1 := TestSystemService()
	s2 := TestPostgresService()
	s3 := TestPgbouncerService()
	r.addService(s1)
	r.addService(s2)
	r.addService(s3)
	assert.Equal(t, 3, r.totalServices())
}

func TestRepository_getService(t *testing.T) {
	r := NewRepository()
	s := TestSystemService()
	r.addService(s)

	got := r.getService(s.ServiceID)
	assert.Equal(t, s.ServiceID, got.ServiceID)
	assert.Equal(t, s.ConnSettings, got.ConnSettings)
}

func TestRepository_getServiceIDs(t *testing.T) {
	r := NewRepository()
	s1 := TestSystemService()
	s2 := TestPostgresService()
	s3 := TestPgbouncerService()
	r.addService(s1)
	r.addService(s2)
	r.addService(s3)

	ids := r.getServiceIDs()
	assert.Equal(t, 3, len(ids))

	contains := func(ss []string, s string) bool {
		for _, val := range ss {
			if val == s {
				return true
			}
		}
		return false
	}

	for _, v := range []string{s1.ServiceID, s2.ServiceID, s3.ServiceID} {
		assert.True(t, contains(ids, v))
	}
}

func TestRepository_addServicesFromConfig(t *testing.T) {
	testCases := []struct {
		name     string
		config   Config
		expected int // total number of services expected in the repo
	}{
		{
			name: "valid",
			config: Config{ConnsSettings: ConnsSettings{
				"test": {ServiceType: model.ServiceTypePostgresql, Conninfo: "host=127.0.0.1 port=5432 user=pgscv dbname=pgscv_fixtures"},
			}},
			expected: 2,
		},
		{
			name:     "empty conn settings",
			config:   Config{},
			expected: 1,
		},
		{
			name:     "invalid service",
			config:   Config{ConnsSettings: ConnsSettings{"test": {ServiceType: model.ServiceTypePostgresql, Conninfo: "invalid conninfo"}}},
			expected: 1,
		},
		{
			name:     "unavailable service",
			config:   Config{ConnsSettings: ConnsSettings{"test": {ServiceType: model.ServiceTypePostgresql, Conninfo: "port=1"}}},
			expected: 1,
		},
	}

	for _, tc := range testCases {
		r := NewRepository()
		r.addServicesFromConfig(tc.config)
		assert.Equal(t, tc.expected, r.totalServices())
	}
}

func TestRepository_setupServices(t *testing.T) {
	testCases := []struct {
		name     string
		config   Config
		expected int // total number of services expected in the repo
	}{
		{
			name: "valid",
			config: Config{
				ConnsSettings: ConnsSettings{
					"test": {ServiceType: model.ServiceTypePostgresql, Conninfo: "host=127.0.0.1 port=5432 user=pgscv dbname=pgscv_fixtures"},
				},
			},
			expected: 2,
		},
	}

	for _, tc := range testCases {
		r := NewRepository()
		r.addServicesFromConfig(tc.config)
		assert.Equal(t, tc.expected, r.totalServices())

		assert.NoError(t, r.setupServices(tc.config))
		s := r.getService("test")
		assert.NotNil(t, s.Collector)

		prometheus.Unregister(s.Collector)
	}
}
