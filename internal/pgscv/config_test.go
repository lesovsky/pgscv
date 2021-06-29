package pgscv

import (
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/filter"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/service"
	"os"
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
			name:  "valid: with filters V2",
			valid: true,
			file:  "testdata/pgscv-filters-example.yaml",
			want: &Config{
				ListenAddress: "127.0.0.1:8080",
				Defaults:      map[string]string{},
				CollectorsSettings: model.CollectorsSettings{
					"postgres/custom": {
						Filters: filter.Filters{
							"device": {Exclude: "^(test|example)$"},
						},
					},
				},
			},
		},
		{
			name:  "valid: with collectors settings",
			valid: true,
			file:  "testdata/pgscv-collectors-settings-example.yaml",
			want: &Config{
				ListenAddress: "127.0.0.1:8080",
				Defaults:      map[string]string{},
				CollectorsSettings: model.CollectorsSettings{
					"postgres/custom": {
						Subsystems: map[string]model.MetricsSubsystem{
							"activity": {
								Query: "select datname as database,xact_commit,xact_rollback,blks_read as read,blks_write as write from pg_stat_database",
								Metrics: model.Metrics{
									{ShortName: "xact_commit_total", Usage: "COUNTER", Labels: []string{"database"}, Value: "xact_commit", Description: "description"},
									{ShortName: "blocks_total", Usage: "COUNTER", Labels: []string{"database"},
										LabeledValues: map[string][]string{"access": {"read", "write"}}, Description: "description",
									},
								},
							},
							"bgwriter": {
								Query: "select maxwritten_clean from pg_stat_bgwriter",
								Metrics: model.Metrics{
									{ShortName: "maxwritten_clean_total", Usage: "COUNTER", Value: "maxwritten_clean", Description: "description"},
								},
							},
						},
					},
				},
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
			name:  "invalid config: invalid databases string",
			valid: false,
			in:    &Config{ListenAddress: "127.0.0.1:8080", Databases: "["},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.in.Validate()
			if tc.valid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func Test_validateCollectorSettings(t *testing.T) {
	testcases := []struct {
		valid    bool
		settings model.CollectorsSettings
	}{
		{valid: true, settings: nil},
		{valid: true, settings: make(map[string]model.CollectorSettings)},
		{
			valid: true,
			settings: map[string]model.CollectorSettings{
				"example/example": {
					Subsystems: map[string]model.MetricsSubsystem{
						"example1": {
							Query: "SELECT 'L1' as label1, 1 as value1",
							Metrics: model.Metrics{
								{ShortName: "v1", Usage: "COUNTER", Value: "value1", Labels: []string{"label1"}, Description: "description"},
							},
						},
						"example2": {
							Query: "SELECT 'L2' as label2, 1 as value1, 2 as value2",
							Metrics: model.Metrics{
								{ShortName: "v1", Usage: "COUNTER", Value: "value1", Labels: []string{"label2"}, Description: "description"},
								{ShortName: "v2", Usage: "GAUGE", Value: "value2", Labels: []string{"label2"}, Description: "description"},
							},
						},
					},
				},
				"example/example2": {
					Subsystems: map[string]model.MetricsSubsystem{
						"example1": {
							Query: "SELECT 'L1' as label1, 1 as value1",
							Metrics: model.Metrics{
								{ShortName: "v1", Usage: "COUNTER", Value: "value1", Labels: []string{"label1"}, Description: "description"},
							},
						},
					},
				},
			},
		},
		// invalid collectors names
		{valid: false, settings: map[string]model.CollectorSettings{"invalid": {}}},
		{valid: false, settings: map[string]model.CollectorSettings{"invalid/": {}}},
		{valid: false, settings: map[string]model.CollectorSettings{"/invalid": {}}},
		{valid: false, settings: map[string]model.CollectorSettings{"example/inva:lid": {}}},
		{
			valid: false, // Invalid subsystem name for metric
			settings: map[string]model.CollectorSettings{
				"example/example": {Subsystems: map[string]model.MetricsSubsystem{"inva:lid": {}}},
			},
		},
		{
			valid: false, // Invalid filters specified
			settings: map[string]model.CollectorSettings{
				"example/example": {
					Filters:    filter.Filters{"test": filter.Filter{Exclude: "["}},
					Subsystems: map[string]model.MetricsSubsystem{"example": {}},
				},
			},
		},
		{
			valid: false, // No query specified when metric exists
			settings: map[string]model.CollectorSettings{
				"example/example": {
					Subsystems: map[string]model.MetricsSubsystem{
						"example1": {
							Metrics: model.Metrics{
								{ShortName: "l1", Usage: "COUNTER", Value: "value1", Description: "description"},
							},
						},
					},
				},
			},
		},
		{
			valid: false, // Invalid name for metric
			settings: map[string]model.CollectorSettings{
				"example/example": {
					Subsystems: map[string]model.MetricsSubsystem{
						"example1": {
							Query: "SELECT 'L1' as label1, 1 as value1",
							Metrics: model.Metrics{
								{ShortName: "inva:lid", Usage: "COUNTER", Value: "value1", Labels: []string{"label1"}, Description: "v1 description"},
							},
						},
					},
				},
			},
		},
		{
			valid: false, // Empty metric descriptor
			settings: map[string]model.CollectorSettings{
				"example/example": {
					Subsystems: map[string]model.MetricsSubsystem{
						"example1": {
							Query: "SELECT 'label1' as l1, 1 as v1",
							Metrics: model.Metrics{
								{ShortName: "v1", Usage: "COUNTER", Value: "v1"},
							},
						},
					},
				},
			},
		},
		{
			valid: false, // Invalid usage
			settings: map[string]model.CollectorSettings{
				"example/example": {
					Subsystems: map[string]model.MetricsSubsystem{
						"example1": {
							Query: "SELECT 'label1' as l1, 1 as v1",
							Metrics: model.Metrics{
								{ShortName: "v1", Value: "v1", Usage: "INVALID"},
							},
						},
					},
				},
			},
		},
		{
			valid: false, // Invalid databases regexp
			settings: map[string]model.CollectorSettings{
				"example/example": {
					Subsystems: map[string]model.MetricsSubsystem{
						"example1": {
							Databases: "[",
						},
					},
				},
			},
		},
		{
			valid: false, // No value, nor labeled_values
			settings: map[string]model.CollectorSettings{
				"example/example": {
					Subsystems: map[string]model.MetricsSubsystem{
						"example1": {
							Query: "SELECT 'L1' as label1, 1 as value1",
							Metrics: model.Metrics{
								{ShortName: "v1", Usage: "COUNTER", Labels: []string{"label1"}, Description: "description"},
							},
						},
					},
				},
			},
		},
		{
			valid: false, // No value, nor labeled_values
			settings: map[string]model.CollectorSettings{
				"example/example": {
					Subsystems: map[string]model.MetricsSubsystem{
						"example1": {
							Query: "SELECT 'L1' as label1, 1 as value1",
							Metrics: model.Metrics{
								{ShortName: "v1", Usage: "COUNTER", Value: "value1", LabeledValues: map[string][]string{"value1": {"test"}}, Labels: []string{"label1"}, Description: "description"},
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range testcases {
		if tc.valid {
			assert.NoError(t, validateCollectorSettings(tc.settings))
		} else {
			assert.Error(t, validateCollectorSettings(tc.settings))
		}
	}
}

func Test_newConfigFromEnv(t *testing.T) {
	testcases := []struct {
		valid   bool
		envvars map[string]string
		want    *Config
	}{
		{
			valid:   true, // No env variables
			envvars: map[string]string{},
			want:    &Config{Defaults: map[string]string{}},
		},
		{
			valid: true, // Completely valid variables
			envvars: map[string]string{
				"PGSCV_LISTEN_ADDRESS":     "127.0.0.1:12345",
				"PGSCV_AUTOUPDATE":         "1",
				"PGSCV_NO_TRACK_MODE":      "yes",
				"PGSCV_SEND_METRICS_URL":   "127.0.0.1:54321",
				"PGSCV_API_KEY":            "example",
				"PGSCV_DATABASES":          "exampledb",
				"PGSCV_DISABLE_COLLECTORS": "example/1,example/2, example/3",
				"POSTGRES_DSN":             "example_dsn",
				"POSTGRES_DSN_EXAMPLE1":    "example_dsn",
				"PGBOUNCER_DSN":            "example_dsn",
				"PGBOUNCER_DSN_EXAMPLE2":   "example_dsn",
				"PATRONI_URL":              "example_url",
				"PATRONI_URL_EXAMPLE3":     "example_url",
			},
			want: &Config{
				ListenAddress:     "127.0.0.1:12345",
				AutoUpdate:        "1",
				NoTrackMode:       true,
				SendMetricsURL:    "127.0.0.1:54321",
				APIKey:            "example",
				Databases:         "exampledb",
				DisableCollectors: []string{"example/1", "example/2", "example/3"},
				ServicesConnsSettings: map[string]service.ConnSetting{
					"postgres":  {ServiceType: model.ServiceTypePostgresql, Conninfo: "example_dsn"},
					"EXAMPLE1":  {ServiceType: model.ServiceTypePostgresql, Conninfo: "example_dsn"},
					"pgbouncer": {ServiceType: model.ServiceTypePgbouncer, Conninfo: "example_dsn"},
					"EXAMPLE2":  {ServiceType: model.ServiceTypePgbouncer, Conninfo: "example_dsn"},
					"patroni":   {ServiceType: model.ServiceTypePatroni, BaseURL: "example_url"},
					"EXAMPLE3":  {ServiceType: model.ServiceTypePatroni, BaseURL: "example_url"},
				},
				Defaults: map[string]string{},
			},
		},
		{
			valid:   false, // Invalid postgres DSN key
			envvars: map[string]string{"POSTGRES_DSN_": "example_dsn"},
		},
		{
			valid:   false, // Invalid pgbouncer DSN key
			envvars: map[string]string{"PGBOUNCER_DSN_": "example_dsn"},
		},
		{
			valid:   false, // Invalid patroni URL key
			envvars: map[string]string{"PATRONI_URL_": "example_dsn"},
		},
	}

	for _, tc := range testcases {
		for k, v := range tc.envvars {
			assert.NoError(t, os.Setenv(k, v))
		}

		got, err := newConfigFromEnv()
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		} else {
			assert.Error(t, err)
		}

		for k := range tc.envvars {
			assert.NoError(t, os.Unsetenv(k))
		}
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

func Test_newDatabasesRegexp(t *testing.T) {
	testcases := []struct {
		valid bool
		str   string
	}{
		{valid: true, str: "example(1|2)"},
		{valid: true, str: ""},
		{valid: false, str: "["},
	}

	for _, tc := range testcases {
		got, err := newDatabasesRegexp(tc.str)
		if tc.valid {
			assert.NoError(t, err)
			assert.NotNil(t, got)
		} else {
			assert.Error(t, err)
			assert.Nil(t, got)
		}
	}
}
