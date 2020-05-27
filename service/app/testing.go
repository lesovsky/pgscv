package app

import (
	"github.com/barcodepro/pgscv/service/model"
	"testing"
)

const (
	// how many metrics in percent might be absent during tests without failing
	absentMetricsThreshold float64 = 10
)

// TestAppConfig returns app config for test purposes.
// Config contains valid but fake data and should not be used in functional tests.
func TestAppExampleConfig(_ *testing.T) *Config {
	return &Config{
		ListenAddress: "127.0.0.1:10090",
		ServicesConnSettings: []model.ServiceConnSetting{
			{ServiceType: "postgres", Conninfo: "host=127.0.0.1 port=5432 user=postgres dbname=postgres password=testpassword"},
			{ServiceType: "postgres", Conninfo: "host=127.0.0.1 port=5433 user=postgres dbname=postgres"},
			{ServiceType: "postgres", Conninfo: "host=/var/run/postgresql port=6432 user=pgbouncer dbname=pgbouncer"},
			{ServiceType: "postgres", Conninfo: "postgres://postgres@127.0.0.1:5432/postgres"},
		},
		Defaults: map[string]string{
			"postgres_username":  "pgscv",
			"postgres_password":  "testpassword",
			"postgres_dbname":    "postgres",
			"pgbouncer_username": "pgscv",
			"pgbouncer_password": "testpassword",
			"pgbouncer_dbname":   "pgbouncer",
		},
	}
}
