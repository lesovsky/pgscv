package service

import (
	"context"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/runtime"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestServiceRepo_addService(t *testing.T) {
	r := NewServiceRepo()
	s1 := TestSystemService()
	s2 := TestPostgresService()
	s3 := TestPgbouncerService()
	r.addService(s1.ServiceID, s1)
	r.addService(s2.ServiceID, s2)
	r.addService(s3.ServiceID, s3)
	assert.Equal(t, 3, r.TotalServices())
}

func TestServiceRepo_getService(t *testing.T) {
	r := NewServiceRepo()
	s := TestSystemService()
	r.addService(s.ServiceID, s)

	got := r.GetService(s.ServiceID)
	assert.Equal(t, s.ServiceID, got.ServiceID)
	assert.Equal(t, s.ProjectID, got.ProjectID)
	assert.Equal(t, s.ConnSettings, got.ConnSettings)
}

func TestServiceRepo_removeServiceByServiceID(t *testing.T) {
	r := NewServiceRepo()
	s := TestSystemService()
	r.addService(s.ServiceID, s)
	assert.Equal(t, 1, r.TotalServices())
	r.removeServiceByServiceID(s.ServiceID)
	assert.Equal(t, 0, r.TotalServices())
}

func TestServiceRepo_getServiceIDs(t *testing.T) {
	r := NewServiceRepo()
	s1 := TestSystemService()
	s2 := TestPostgresService()
	s3 := TestPgbouncerService()
	r.addService(s1.ServiceID, s1)
	r.addService(s2.ServiceID, s2)
	r.addService(s3.ServiceID, s3)

	ids := r.GetServiceIDs()
	assert.Equal(t, 3, len(ids))

	for _, v := range []string{s1.ServiceID, s2.ServiceID, s3.ServiceID} {
		assert.True(t, stringsContains(ids, v))
	}
}

func TestServiceRepo_addServicesFromConfig(t *testing.T) {
	testCases := []struct {
		name     string
		config   Config
		expected int // total number of services expected in the repo
	}{
		{
			name: "valid",
			config: Config{ConnSettings: []ServiceConnSetting{
				{ServiceType: "postgres", Conninfo: "host=127.0.0.1 port=5432 user=pgscv dbname=postgres"},
			}},
			expected: 2,
		},
		{
			name:     "empty conn settings",
			config:   Config{},
			expected: 0,
		},
		{
			name:     "invalid service",
			config:   Config{ConnSettings: []ServiceConnSetting{{ServiceType: "postgres", Conninfo: "invalid conninfo"}}},
			expected: 1,
		},
		{
			name:     "unavailable service",
			config:   Config{ConnSettings: []ServiceConnSetting{{ServiceType: "postgres", Conninfo: "port=15432"}}},
			expected: 1,
		},
	}

	for _, tc := range testCases {
		r := NewServiceRepo()
		r.AddServicesFromConfig(tc.config)
		assert.Equal(t, tc.expected, r.TotalServices())
	}
}

func TestServiceRepo_setupServices(t *testing.T) {
	testCases := []struct {
		name     string
		config   Config
		expected int // total number of services expected in the repo
	}{
		{
			name: "valid",
			config: Config{
				ConnSettings: []ServiceConnSetting{
					{ServiceType: "postgres", Conninfo: "host=127.0.0.1 port=5432 user=pgscv dbname=postgres"},
				},
			},
			expected: 2,
		},
		{
			name: "valid with pull mode",
			config: Config{
				RuntimeMode: runtime.PullMode,
				ConnSettings: []ServiceConnSetting{
					{ServiceType: "postgres", Conninfo: "host=127.0.0.1 port=5432 user=pgscv dbname=postgres"},
				},
			},
			expected: 2,
		},
	}

	for _, tc := range testCases {
		r := NewServiceRepo()
		r.AddServicesFromConfig(tc.config)
		assert.Equal(t, tc.expected, r.TotalServices())

		assert.NoError(t, r.SetupServices(tc.config))
		// TODO: should be enabled after implementing postgres/pgbouncer services
		//s := r.GetService("postgres:127.0.0.1:5432")
		//assert.NotNil(t, s.Collector)
	}
}

func TestServiceRepo_startBackgroundDiscovery(t *testing.T) {
	r := NewServiceRepo()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	r.StartBackgroundDiscovery(ctx, Config{})
	assert.NotEqual(t, 0, r.TotalServices())
}

func TestServiceRepo_lookupServices(t *testing.T) {
	if uid := os.Geteuid(); uid != 0 {
		t.Skipf("root privileges required, skip")
	}

	r := NewServiceRepo()
	assert.NoError(t, r.lookupServices(Config{}))
	assert.NotEqual(t, 0, r.TotalServices())
}

func Test_parsePostgresProcessCmdline(t *testing.T) {
	testCases := []struct {
		valid    bool
		payload  []string
		expected string
	}{
		{valid: true, payload: []string{"/bin/postgres", "-D", "/data", "-c", "config_file=/postgresql.conf"}, expected: "/data"},
		{valid: false, payload: []string{"/bin/true", "-f", "config_file=/postgresql.conf"}},
		{valid: false, payload: []string{"/bin/true", "-D"}},
	}

	for _, tc := range testCases {
		s, err := parsePostgresProcessCmdline(tc.payload)
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, s)
		} else {
			assert.Error(t, err)
			assert.Empty(t, s)
		}
	}
}

func Test_newPostgresConnectionParams(t *testing.T) {
	testCases := []struct {
		name     string
		valid    bool
		expected string // stuff we expected to get in valid testcase
		golden   string
	}{
		{
			name:     "valid postmaster.pid",
			valid:    true,
			expected: "127.0.0.1",
			golden:   "valid",
		},
		{
			name:     "valid postmaster.pid with listening on UNIX socket",
			valid:    true,
			expected: "",
			golden:   "valid-unix",
		},
		{
			name:   "invalid postmaster.pid",
			valid:  false,
			golden: "invalid",
		},
		{
			name:   "non-existent postmaster.pid",
			valid:  false,
			golden: "unknown",
		},
		{
			name:   "invalid timestamp in postmaster.pid",
			valid:  false,
			golden: "invalid-ts",
		},
		{
			name:   "invalid port in postmaster.pid",
			valid:  false,
			golden: "invalid-port",
		},
	}

	for _, tc := range testCases {
		cp, err := newPostgresConnectionParams("testdata/postmaster.pid.d/" + tc.golden + ".golden")
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, "/var/lib/postgresql/12/main", cp.datadirPath)
			assert.Equal(t, "/var/run/postgresql", cp.unixSocketDirPath)
			assert.Equal(t, tc.expected, cp.listenAddr)
			assert.Equal(t, 5432, cp.listenPort)
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_newPostgresConnectionString(t *testing.T) {
	testCases := []struct {
		name     string
		payload  connectionParams
		defaults map[string]string
		unix     bool
		expected string
	}{
		{
			name:     "all empty",
			payload:  connectionParams{},
			defaults: map[string]string{},
			unix:     true,
			expected: "application_name=pgscv user=pgscv dbname=postgres",
		},
		{
			name:     "all empty",
			payload:  connectionParams{},
			defaults: map[string]string{},
			unix:     false,
			expected: "application_name=pgscv user=pgscv dbname=postgres",
		},
		{
			name:     "unix socket",
			payload:  connectionParams{unixSocketDirPath: "/run"},
			defaults: map[string]string{},
			unix:     true,
			expected: "application_name=pgscv host=/run user=pgscv dbname=postgres",
		},
		{
			name:     "listen addr",
			payload:  connectionParams{listenAddr: "1.2.3.4"},
			defaults: map[string]string{},
			unix:     false,
			expected: "application_name=pgscv host=1.2.3.4 user=pgscv dbname=postgres",
		},
		{
			name:     "listen port",
			payload:  connectionParams{listenPort: 1234},
			defaults: map[string]string{},
			unix:     false,
			expected: "application_name=pgscv port=1234 user=pgscv dbname=postgres",
		},
		{
			name:     "defaults: username",
			payload:  connectionParams{},
			defaults: map[string]string{"postgres_username": "exampleuser"},
			unix:     false,
			expected: "application_name=pgscv user=exampleuser dbname=postgres",
		},
		{
			name:     "defaults: dbname",
			payload:  connectionParams{},
			defaults: map[string]string{"postgres_dbname": "exampledb"},
			unix:     false,
			expected: "application_name=pgscv user=pgscv dbname=exampledb",
		},
		{
			name:     "defaults: password",
			payload:  connectionParams{},
			defaults: map[string]string{"postgres_password": "examplepass"},
			unix:     false,
			expected: "application_name=pgscv user=pgscv dbname=postgres password=examplepass",
		},
		{
			name:     "full (tcp)",
			payload:  connectionParams{listenAddr: "1.2.3.4", listenPort: 1234, unixSocketDirPath: "/run"},
			defaults: map[string]string{"postgres_username": "exampleuser", "postgres_dbname": "exampledb", "postgres_password": "examplepass"},
			unix:     false,
			expected: "application_name=pgscv host=1.2.3.4 port=1234 user=exampleuser dbname=exampledb password=examplepass",
		},
		{
			name:     "full (unix)",
			payload:  connectionParams{listenAddr: "1.2.3.4", listenPort: 1234, unixSocketDirPath: "/run"},
			defaults: map[string]string{"postgres_username": "exampleuser", "postgres_dbname": "exampledb", "postgres_password": "examplepass"},
			unix:     true,
			expected: "application_name=pgscv host=/run port=1234 user=exampleuser dbname=exampledb password=examplepass",
		},
	}

	for _, tc := range testCases {
		got := newPostgresConnectionString(tc.payload, tc.defaults, tc.unix)
		assert.Equal(t, tc.expected, got)
	}
}

func Test_parsePgbouncerIniFile(t *testing.T) {
	testCases := []struct {
		name     string
		valid    bool
		expected map[string]interface{} // stuff we expected to get in valid testcase
		golden   string
	}{
		{
			name:     "valid pgbouncer.ini",
			valid:    true,
			expected: map[string]interface{}{"listen_addr": "1.2.3.4", "listen_port": 16432, "unix_socket_dir": "/testdir"},
			golden:   "valid",
		},
		{
			name:     "valid pgbouncer.ini with listen_addr asterisk",
			valid:    true,
			expected: map[string]interface{}{"listen_addr": "127.0.0.1", "listen_port": 16432, "unix_socket_dir": "/testdir"},
			golden:   "valid-asterisk",
		},
		{
			name:     "valid trimmed pgbouncer.ini",
			valid:    true,
			expected: map[string]interface{}{"listen_addr": "1.2.3.4", "listen_port": 16432, "unix_socket_dir": "/testdir"},
			golden:   "valid-trim",
		},
		{
			name:     "valid pgbouncer.ini, incomplete line format",
			valid:    true,
			expected: map[string]interface{}{"listen_addr": "", "listen_port": 16432, "unix_socket_dir": "/testdir"},
			golden:   "valid-incomplete",
		},
		{
			name:     "valid pgbouncer.ini, incomplete line format",
			valid:    true,
			expected: map[string]interface{}{"listen_addr": "1.2.3.4", "listen_port": 6432, "unix_socket_dir": "/testdir"},
			golden:   "valid-non-value-param",
		},
		{
			name:     "valid pgbouncer.ini, all commented",
			valid:    true,
			expected: map[string]interface{}{"listen_addr": "", "listen_port": 6432, "unix_socket_dir": "/tmp"},
			golden:   "valid-commented",
		},
		{
			name:   "non-existent pgbouncer.ini",
			valid:  false,
			golden: "non-existent",
		},
		{
			name:   "invalid pgbouncer.ini, incomplete line format",
			valid:  false,
			golden: "invalid-port",
		},
	}

	log.SetLevel("info")
	for _, tc := range testCases {
		cp, err := parsePgbouncerIniFile("testdata/pgbouncer.ini.d/" + tc.golden + ".golden")
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.expected["listen_addr"], cp.listenAddr)
			assert.Equal(t, tc.expected["listen_port"], cp.listenPort)
			assert.Equal(t, tc.expected["unix_socket_dir"], cp.unixSocketDirPath)
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_newPgbouncerConnectionString(t *testing.T) {
	testCases := []struct {
		name     string
		payload  connectionParams
		defaults map[string]string
		expected string
	}{
		{
			name:     "all empty",
			payload:  connectionParams{},
			defaults: map[string]string{},
			expected: "application_name=pgscv user=pgscv dbname=pgbouncer",
		},
		{
			name:     "unix socket",
			payload:  connectionParams{unixSocketDirPath: "/testdir"},
			defaults: map[string]string{},
			expected: "application_name=pgscv host=/testdir user=pgscv dbname=pgbouncer",
		},
		{
			name:     "listen addr",
			payload:  connectionParams{listenAddr: "1.2.3.4"},
			defaults: map[string]string{},
			expected: "application_name=pgscv host=1.2.3.4 user=pgscv dbname=pgbouncer",
		},
		{
			name:     "port",
			payload:  connectionParams{listenAddr: "1.2.3.4", listenPort: 16432},
			defaults: map[string]string{},
			expected: "application_name=pgscv host=1.2.3.4 port=16432 user=pgscv dbname=pgbouncer",
		},
		{
			name:     "defaults: username",
			payload:  connectionParams{},
			defaults: map[string]string{"pgbouncer_username": "exampleuser"},
			expected: "application_name=pgscv user=exampleuser dbname=pgbouncer",
		},
		{
			name:     "defaults: password",
			payload:  connectionParams{},
			defaults: map[string]string{"pgbouncer_password": "examplepass"},
			expected: "application_name=pgscv user=pgscv dbname=pgbouncer password=examplepass",
		},
		{
			name:     "full (unix)",
			payload:  connectionParams{listenPort: 1234, unixSocketDirPath: "/run"},
			defaults: map[string]string{"pgbouncer_username": "exampleuser", "pgbouncer_password": "examplepass"},
			expected: "application_name=pgscv host=/run port=1234 user=exampleuser dbname=pgbouncer password=examplepass",
		},
		{
			name:     "full (tcp)",
			payload:  connectionParams{listenAddr: "1.2.3.4", listenPort: 1234, unixSocketDirPath: "/run"},
			defaults: map[string]string{"pgbouncer_username": "exampleuser", "pgbouncer_password": "examplepass"},
			expected: "application_name=pgscv host=1.2.3.4 port=1234 user=exampleuser dbname=pgbouncer password=examplepass",
		},
	}

	for _, tc := range testCases {
		got := newPgbouncerConnectionString(tc.payload, tc.defaults)
		assert.Equal(t, tc.expected, got)
	}
}

func Test_attemptConnect(t *testing.T) {
	assert.NoError(t, attemptConnect("host=127.0.0.1 port=5432 user=postgres dbname=postgres"))
	assert.Error(t, attemptConnect("host=127.0.0.1 port=12345 user=invalid dbname=invalid"))
}
