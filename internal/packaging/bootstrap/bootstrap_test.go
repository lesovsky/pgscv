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

func Test_createDirectoryTree(t *testing.T) {
	var username string
	if username = os.Getenv("USER"); username == "" {
		username = "root"
	}

	testcases := []struct {
		valid  bool
		config Config
	}{
		{valid: true, config: Config{RunAsUser: username, Prefix: "/tmp", Bindir: "/tmp/pgscv/bin"}},
		{valid: false, config: Config{RunAsUser: username, Prefix: "tmp"}},
	}

	for _, tc := range testcases {
		err := createDirectoryTree(tc.config)
		if tc.valid {
			assert.NoError(t, err)
			assert.NoError(t, os.RemoveAll(tc.config.Prefix+"/pgscv"))
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_createConfigFile(t *testing.T) {
	var username string
	if username = os.Getenv("USER"); username == "" {
		username = "root"
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
				ConfigFile: "/tmp/pgscv.yaml",
				RunAsUser:  username, APIKey: "TEST1234TEST-TEST-1234-TEST1234",
				SendMetricsURL: "http://127.0.0.1:9091", AutoUpdateEnv: "true",
			},
		},
		{
			name:  "invalid: unknown user",
			valid: false,
			in:    Config{RunAsUser: "unknown"},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			err := createConfigFile(tc.in)
			if tc.valid {
				assert.NoError(t, err)
				assert.FileExists(t, tc.in.ConfigFile)
				assert.NoError(t, os.Remove(tc.in.ConfigFile))
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func Test_createSystemdUnit(t *testing.T) {
	var username string
	if username = os.Getenv("USER"); username == "" {
		username = "root"
	}

	var testcases = []struct {
		name   string
		valid  bool
		config Config
	}{
		{
			name:   "valid",
			valid:  true,
			config: Config{ExecutableName: "pgscv", SystemdUnit: "/tmp/pgscv.service", RunAsUser: username},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			err := createSystemdUnit(tc.config)
			if tc.valid {
				assert.NoError(t, err)
				assert.FileExists(t, tc.config.SystemdUnit)
				assert.NoError(t, os.Remove(tc.config.SystemdUnit))
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func Test_getUserIDs(t *testing.T) {
	var username string
	if username = os.Getenv("USER"); username == "" {
		username = "root"
	}

	uid, gid, err := getUserIDs(username)
	assert.NoError(t, err)
	assert.Greater(t, uid, -1)
	assert.Greater(t, gid, -1)

	_, _, err = getUserIDs("invalid")
	assert.Error(t, err)
}
