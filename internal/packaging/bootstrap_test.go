package packaging

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestBootstrapConfig_Validate(t *testing.T) {
	var testcases = []struct {
		name  string
		valid bool
		in    BootstrapConfig
	}{
		{
			name:  "valid config",
			valid: true,
			in: BootstrapConfig{
				RunAsUser:            os.Getenv("USER"),
				MetricServiceBaseURL: "http://127.0.0.1:9091", AutoUpdateURL: "http://127.0.0.1:1081",
				APIKey: "TEST1234TEST-TEST-1234-TEST1234", ProjectID: "1",
			},
		},
		{
			name:  "valid config: empty RunAsUser",
			valid: true,
			in: BootstrapConfig{
				MetricServiceBaseURL: "http://127.0.0.1:9091", AutoUpdateURL: "http://127.0.0.1:1081",
				APIKey: "TEST1234TEST-TEST-1234-TEST1234", ProjectID: "1",
			},
		},
		{
			name:  "invalid config: unknown user",
			valid: false,
			in: BootstrapConfig{
				RunAsUser:            "unknown",
				MetricServiceBaseURL: "http://127.0.0.1:9091", AutoUpdateURL: "http://127.0.0.1:1081",
				APIKey: "TEST1234TEST-TEST-1234-TEST1234", ProjectID: "1",
			},
		},
		{
			name:  "invalid config: empty MetricServiceBaseURL",
			valid: false,
			in: BootstrapConfig{
				RunAsUser: os.Getenv("USER"), AutoUpdateURL: "http://127.0.0.1:1081", APIKey: "TEST1234TEST-TEST-1234-TEST1234", ProjectID: "1",
			},
		},
		{
			name:  "invalid config: empty AutoUpdateURL",
			valid: false,
			in: BootstrapConfig{
				RunAsUser: os.Getenv("USER"), MetricServiceBaseURL: "http://127.0.0.1:9091", APIKey: "TEST1234TEST-TEST-1234-TEST1234", ProjectID: "1",
			},
		},
		{
			name:  "invalid config: empty APIKey",
			valid: false,
			in: BootstrapConfig{
				RunAsUser: os.Getenv("USER"), MetricServiceBaseURL: "http://127.0.0.1:9091", AutoUpdateURL: "http://127.0.0.1:1081", ProjectID: "1",
			},
		},
		{
			name:  "invalid config: zero ProjectID",
			valid: false,
			in: BootstrapConfig{
				RunAsUser: os.Getenv("USER"), MetricServiceBaseURL: "http://127.0.0.1:9091", AutoUpdateURL: "http://127.0.0.1:1081", APIKey: "TEST1234TEST-TEST-1234-TEST1234",
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
	var testcases = []struct {
		name  string
		valid bool
		in    BootstrapConfig
	}{
		{
			name:  "valid",
			valid: true,
			in: BootstrapConfig{
				ExecutableName: "testexec", configPathPrefix: "/tmp",
				RunAsUser: os.Getenv("USER"), APIKey: "TEST1234TEST-TEST-1234-TEST1234", ProjectID: "1",
				MetricServiceBaseURL: "http://127.0.0.1:9091", AutoUpdateURL: "http://127.0.0.1:1081",
			},
		},
		{
			name:  "invalid: unknown user",
			valid: false,
			in:    BootstrapConfig{RunAsUser: "unknown"},
		},
		{
			name:  "invalid: invalid configPathPrefix",
			valid: false,
			in: BootstrapConfig{
				RunAsUser: os.Getenv("USER"), configPathPrefix: "/invalid",
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			err := createConfigFile(&tc.in)
			if tc.valid {
				assert.NoError(t, err)
				assert.FileExists(t, "/tmp/testexec.json")
				assert.NoError(t, os.Remove("/tmp/testexec.json"))
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func Test_createSystemdUnit(t *testing.T) {
	var testcases = []struct {
		name  string
		valid bool
		in    BootstrapConfig
	}{
		{
			name:  "valid",
			valid: true,
			in:    BootstrapConfig{ExecutableName: "pgscv", systemdPathPrefix: "/tmp", RunAsUser: os.Getenv("USER")},
		},
		{
			name:  "invalid: unknown user",
			valid: false,
			in:    BootstrapConfig{RunAsUser: "unknown"},
		},
		{
			name:  "invalid: invalid systemdPathPrefix",
			valid: false,
			in:    BootstrapConfig{ExecutableName: "pgscv", systemdPathPrefix: "/invalid", RunAsUser: os.Getenv("USER")},
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
	uid, gid, err := getUserIDs(os.Getenv("USER"))
	assert.NoError(t, err)
	assert.Greater(t, uid, 0)
	assert.Greater(t, gid, 0)

	_, _, err = getUserIDs("invalid")
	assert.Error(t, err)
}
