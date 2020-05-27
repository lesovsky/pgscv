package app

import (
	"github.com/barcodepro/pgscv/service/model"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConfig_Validate(t *testing.T) {
	// test completely filled config
	c := TestAppExampleConfig(t)
	assert.NoError(t, c.Validate())
	assert.Len(t, c.ServicesConnSettings, 4)

	/* test push/pull modes */
	// PULL mode should be used when metrics service URL is not specified
	c = &Config{}
	assert.NoError(t, c.Validate())
	assert.Equal(t, runtimeModePull, c.RuntimeMode)

	// PUSH mode should be used when metrics service URL is specified with API key
	c = &Config{MetricsServiceURL: "http://127.0.0.1", APIKey: "DEMODEMODEMO-1234-1234-DEMO1234"}
	assert.NoError(t, c.Validate())
	assert.Equal(t, runtimeModePush, c.RuntimeMode)
	assert.Equal(t, "1234", c.ProjectID)

	// should fail if metrics service URL is specified without API key
	c = &Config{MetricsServiceURL: "http://127.0.0.1"}
	assert.Error(t, c.Validate())

	// should fail if metrics service URL is specified without invalid API key
	c = &Config{MetricsServiceURL: "http://127.0.0.1", APIKey: "__invalid__"}
	assert.Error(t, c.Validate())

	/* test defaults */
	// test completely empty config, almost all values should be filled by default values
	c = &Config{}
	assert.NoError(t, c.Validate())
	assert.Equal(t, defaultListenAddress, c.ListenAddress)
	assert.Equal(t, defaultPostgresUsername, c.Defaults["postgres_username"])
	assert.Equal(t, defaultPostgresDbname, c.Defaults["postgres_dbname"])
	assert.Equal(t, defaultPgbouncerUsername, c.Defaults["pgbouncer_username"])
	assert.Equal(t, defaultPgbouncerDbname, c.Defaults["pgbouncer_dbname"])

	/* test service connections */
	// test with invalid connection string, it should be removed
	c = &Config{
		ServicesConnSettings: []model.ServiceConnSetting{
			{ServiceType: "postgres", Conninfo: "host=127.0.0.1 port=5432 username=postgres dbname=postgres password=lessqqmorepewpew"},
			{ServiceType: "postgres", Conninfo: "completely_invalid_string"},
		},
	}
	assert.NoError(t, c.Validate())
	assert.Len(t, c.ServicesConnSettings, 1)

	c = &Config{
		ServicesConnSettings: []model.ServiceConnSetting{
			{ServiceType: "postgres", Conninfo: "completely_invalid_string"},
			{ServiceType: "postgres", Conninfo: "another invalid string"},
		},
	}
	assert.NoError(t, c.Validate())
	assert.Nil(t, c.ServicesConnSettings)
}

func TestDecodeProjectID(t *testing.T) {
	testcases := []struct {
		valid bool
		key   string
		id    string
	}{
		// valid keys
		{key: "A1B2C3D4E5F6-A1B2-C3D4-ABCD1EFG", id: "1", valid: true},
		{key: "A1B2C3D4E5F6-A1B2-C3D4-A2CDE5FG", id: "25", valid: true},
		{key: "A1B2C3D4E5F6-A1B2-C3D4-3AB2CD9E", id: "329", valid: true},
		{key: "A1B2C3D4E5F6-A1B2-C3D4-A4B6F1A8", id: "4618", valid: true},
		{key: "A1B2C3D4E5F6-A1B2-C3D4-51D82EA6", id: "51826", valid: true},
		{key: "A1B2C3D4E5F6-A1B2-C3D4-F1581F63", id: "158163", valid: true},
		{key: "A1B2C3D4E5F6-A1B2-C3D4-517B2641", id: "5172641", valid: true},
		{key: "A1B2C3D4E5F6-A1B2-C3D4-81920172", id: "81920172", valid: true},
		// not-compatible length of segments
		{key: "A1B2C3D4E5F-A1B2-C3D4-ABCD1EFG", id: "", valid: false},
		{key: "A1B2C3D4E5F6-1B2-C3D4-ABCD1EFG", id: "", valid: false},
		{key: "A1B2C3D4E5F6-A1B2-3D4-ABCD1EFG", id: "", valid: false},
		{key: "A1B2C3D4E5F6-A1B2-C3D4-BCD1EFG", id: "", valid: false},
		// not-compatible with regexp [A-Z0-9]
		{key: "a1B2C3D4E5F6-A1B2-C3D4-ABCD1EFG", id: "", valid: false},
		{key: "A1B2C3D4E5F6-a1B2-C3D4-ABCD1EFG", id: "", valid: false},
		{key: "A1B2C3D4E5F6-A1B2-c3D4-ABCD1EFG", id: "", valid: false},
		{key: "A1B2C3D4E5F6-A1B2-C3D4-aBCD1EFG", id: "", valid: false},
		// invalid input
		{key: "A1B2C3D4E5F6-A1B2-C3D4", id: "", valid: false},
		{key: "A1B2C3D4E5F6-A1B2-C3D4-ABCD1EFG-A1B2", id: "", valid: false},
		{key: "invalid", id: "", valid: false},
		{key: "", id: "", valid: false},
	}
	for _, tc := range testcases {
		t.Run(tc.key, func(t *testing.T) {
			got, err := newProjectID(tc.key)
			if tc.valid {
				assert.NoError(t, err)
				assert.Equal(t, tc.id, got)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
