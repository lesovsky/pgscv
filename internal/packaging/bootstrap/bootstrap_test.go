package bootstrap

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestConfig_Validate(t *testing.T) {
	var user string
	if user = os.Getenv("USER"); user == "" {
		user = "root"
	}
	var testcases = []struct {
		name  string
		valid bool
		in    Config
	}{
		{
			name:  "valid config",
			valid: true,
			in: Config{
				RunAsUser:      user,
				SendMetricsURL: "http://127.0.0.1:9091", AutoUpdateEnv: "true",
				APIKey: "TEST1234TEST-TEST-1234-TEST1234",
			},
		},
		{
			name:  "valid config: empty RunAsUser",
			valid: true,
			in: Config{
				SendMetricsURL: "http://127.0.0.1:9091", AutoUpdateEnv: "false",
				APIKey: "TEST1234TEST-TEST-1234-TEST1234",
			},
		},
		{
			name:  "invalid config: unknown user",
			valid: false,
			in: Config{
				RunAsUser:      "unknown",
				SendMetricsURL: "http://127.0.0.1:9091", AutoUpdateEnv: "true",
				APIKey: "TEST1234TEST-TEST-1234-TEST1234",
			},
		},
		{
			name:  "invalid config: empty MetricServiceBaseURL",
			valid: false,
			in: Config{
				RunAsUser: user, AutoUpdateEnv: "true", APIKey: "TEST1234TEST-TEST-1234-TEST1234",
			},
		},
		{
			name:  "invalid config: empty AutoUpdateEnv",
			valid: false,
			in: Config{
				RunAsUser: user, SendMetricsURL: "http://127.0.0.1:9091", APIKey: "TEST1234TEST-TEST-1234-TEST1234",
			},
		},
		{
			name:  "invalid config: invalid AutoUpdateEnv",
			valid: false,
			in: Config{
				RunAsUser: user, SendMetricsURL: "http://127.0.0.1:9091", AutoUpdateEnv: "invalid", APIKey: "TEST1234TEST-TEST-1234-TEST1234",
			},
		},
		{
			name:  "invalid config: empty APIKey",
			valid: false,
			in: Config{
				RunAsUser: user, SendMetricsURL: "http://127.0.0.1:9091", AutoUpdateEnv: "true",
			},
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

func Test_createConfigFile(t *testing.T) {
	var user string
	if user = os.Getenv("USER"); user == "" {
		user = "root"
	}
	var testcases = []struct {
		name  string
		valid bool
		in    Config
	}{
		{
			name:  "valid",
			valid: true,
			in: Config{
				ExecutableName: "testexec", configPathPrefix: "/tmp",
				RunAsUser: user, APIKey: "TEST1234TEST-TEST-1234-TEST1234",
				SendMetricsURL: "http://127.0.0.1:9091", AutoUpdateEnv: "true",
			},
		},
		{
			name:  "invalid: unknown user",
			valid: false,
			in:    Config{RunAsUser: "unknown"},
		},
		{
			name:  "invalid: invalid configPathPrefix",
			valid: false,
			in: Config{
				RunAsUser: user, configPathPrefix: "/invalid",
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			err := createConfigFile(&tc.in)
			if tc.valid {
				assert.NoError(t, err)
				assert.FileExists(t, "/tmp/testexec.yaml")
				assert.NoError(t, os.Remove("/tmp/testexec.yaml"))
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func Test_createSystemdUnit(t *testing.T) {
	var user string
	if user = os.Getenv("USER"); user == "" {
		user = "root"
	}
	var testcases = []struct {
		name  string
		valid bool
		in    Config
	}{
		{
			name:  "valid",
			valid: true,
			in:    Config{ExecutableName: "pgscv", systemdPathPrefix: "/tmp", RunAsUser: user},
		},
		{
			name:  "invalid: invalid systemdPathPrefix",
			valid: false,
			in:    Config{ExecutableName: "pgscv", systemdPathPrefix: "/invalid", RunAsUser: user},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			err := createSystemdUnit(&tc.in)
			if tc.valid {
				assert.NoError(t, err)
				assert.FileExists(t, "/tmp/pgscv.service")
				assert.NoError(t, os.Remove("/tmp/pgscv.service"))
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func Test_getUserIDs(t *testing.T) {
	var user string
	if user = os.Getenv("USER"); user == "" {
		user = "root"
	}

	uid, gid, err := getUserIDs(user)
	assert.NoError(t, err)
	assert.Greater(t, uid, -1)
	assert.Greater(t, gid, -1)

	_, _, err = getUserIDs("invalid")
	assert.Error(t, err)
}
