package pgscv

import (
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/filter"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/service"
	"testing"
)

func TestNewConfig(t *testing.T) {
	var testcases = []struct {
		name  string
		valid bool
		file  string
		want  *Config
	}{
		{
			name:  "valid: pull-only example",
			valid: true,
			file:  "testdata/pgscv-pull-example.yaml",
			want: &Config{
				ListenAddress: "127.0.0.1:8080",
				Defaults:      map[string]string{},
			},
		},
		{
			name:  "valid: pull/push example",
			valid: true,
			file:  "testdata/pgscv-push-example.yaml",
			want: &Config{
				APIKey:         "TEST1234TEST-TEST-1234-TEST1234",
				SendMetricsURL: "http://127.0.0.1:9091",
				Defaults:       map[string]string{},
			},
		},
		{
			name:  "valid: with defaults",
			valid: true,
			file:  "testdata/pgscv-defaults-example.yaml",
			want: &Config{
				ListenAddress: "127.0.0.1:8080",
				Defaults: map[string]string{
					"postgres_username": "testuser", "postgres_password": "testpassword",
					"pgbouncer_username": "testuser2", "pgbouncer_password": "testapassword2",
				},
			},
		},
		{
			name:  "valid: with services",
			valid: true,
			file:  "testdata/pgscv-services-example.yaml",
			want: &Config{
				ListenAddress: "127.0.0.1:8080",
				Defaults:      map[string]string{},
				ServicesConnsSettings: service.ConnsSettings{
					"postgres:5432":  {ServiceType: model.ServiceTypePostgresql, Conninfo: "host=127.0.0.1 port=5432 dbname=pgscv_fixtures user=pgscv"},
					"pgbouncer:6432": {ServiceType: model.ServiceTypePgbouncer, Conninfo: "host=127.0.0.1 port=6432 dbname=pgbouncer user=pgscv"},
				},
			},
		},
		{
			name:  "valid: with filters",
			valid: true,
			file:  "testdata/pgscv-filters-example.yaml",
			want: &Config{
				ListenAddress: "127.0.0.1:8080",
				Defaults:      map[string]string{},
				Filters: map[string]filter.Filter{
					"diskstats/device": {Exclude: "^(test|example)$"},
					"netdev/device":    {Include: "^(test|example)$"},
				},
			},
		},
		{
			name:  "empty config-file opt",
			valid: true,
			file:  "",
			want: &Config{
				Defaults: map[string]string{},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewConfig(tc.file)
			if tc.valid {
				assert.NoError(t, err)
				assert.Equal(t, tc.want, got)
			} else {
				assert.Error(t, err)
			}
		})
	}

	// try to open unknown file
	_, err := NewConfig("testdata/nonexistent.yaml")
	assert.Error(t, err)

	// try to open invalid file
	_, err = NewConfig("testdata/invalid.txt")
	assert.Error(t, err)
}

func TestConfig_Validate(t *testing.T) {
	var testcases = []struct {
		name  string
		valid bool
		in    *Config
	}{
		{
			name:  "valid config for PULL Mode",
			valid: true,
			in:    &Config{ListenAddress: "127.0.0.1:8080"},
		},
		{
			name:  "valid config for PUSH Mode",
			valid: true,
			in:    &Config{SendMetricsURL: "http://127.0.0.1:9091", APIKey: "TEST1234TEST-TEST-1234-TEST1234"},
		},
		{
			name:  "invalid config for PUSH Mode: no api key present",
			valid: false,
			in:    &Config{SendMetricsURL: "http://127.0.0.1:9091"},
		},
		{
			name:  "invalid config for PUSH Mode: empty api key",
			valid: false,
			in:    &Config{SendMetricsURL: "http://127.0.0.1:9091", APIKey: ""},
		},
		{
			name:  "valid config with specified services",
			valid: true,
			in: &Config{ListenAddress: "127.0.0.1:8080", ServicesConnsSettings: service.ConnsSettings{
				"postgres:5432":  {ServiceType: model.ServiceTypePostgresql, Conninfo: "host=127.0.0.1 dbname=pgscv_fixtures user=pgscv"},
				"pgbouncer:6432": {ServiceType: model.ServiceTypePgbouncer, Conninfo: "host=127.0.0.1 port=6432 dbname=pgbouncer user=pgscv"},
			}},
		},
		{
			name:  "valid with enabled auto-update",
			valid: true,
			in:    &Config{AutoUpdate: "stable"},
		},
		{
			name:  "invalid with wrong auto-update value",
			valid: false,
			in:    &Config{AutoUpdate: "invalid"},
		},
		{
			name:  "invalid config with specified services: empty service type",
			valid: false,
			in: &Config{ListenAddress: "127.0.0.1:8080", ServicesConnsSettings: service.ConnsSettings{
				"": {ServiceType: "postgres", Conninfo: "host=127.0.0.1 dbname=pgscv_fixtures user=pgscv"},
			}},
		},
		{
			name:  "invalid config with specified services: empty service type",
			valid: false,
			in: &Config{ListenAddress: "127.0.0.1:8080", ServicesConnsSettings: service.ConnsSettings{
				"test": {ServiceType: "", Conninfo: "host=127.0.0.1 dbname=pgscv_fixtures user=pgscv"},
			}},
		},
		{
			name:  "invalid config with specified services: invalid conninfo",
			valid: false,
			in: &Config{ListenAddress: "127.0.0.1:8080", ServicesConnsSettings: service.ConnsSettings{
				"test": {ServiceType: model.ServiceTypePostgresql, Conninfo: "invalid"},
			}},
		},
		{
			name:  "invalid config: invalid filter",
			valid: false,
			in:    &Config{ListenAddress: "127.0.0.1:8080", Filters: map[string]filter.Filter{"test": {Include: "["}}},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.in.Validate()
			if tc.valid {
				assert.NoError(t, err)
				assert.NotNil(t, tc.in.Filters)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func Test_toggleAutoupdate(t *testing.T) {
	testcases := []struct {
		valid bool
		in    string
		want  string
	}{
		{valid: true, in: "", want: "off"},
		{valid: true, in: "off", want: "off"},
		{valid: true, in: "devel", want: "devel"},
		{valid: true, in: "stable", want: "stable"},
		{valid: false, in: "invalid"},
	}

	for _, tc := range testcases {
		got, err := toggleAutoupdate(tc.in)
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		} else {
			assert.Error(t, err)
		}
	}
}
