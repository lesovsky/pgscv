package service

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_ParsePostgresDSNEnv(t *testing.T) {
	gotID, gotCS, err := ParsePostgresDSNEnv("POSTGRES_DSN", "conninfo")
	assert.NoError(t, err)
	assert.Equal(t, "postgres", gotID)
	assert.Equal(t, ConnSetting{ServiceType: "postgres", Conninfo: "conninfo"}, gotCS)

	gotID, gotCS, err = ParsePostgresDSNEnv("DATABASE_DSN", "conninfo")
	assert.NoError(t, err)
	assert.Equal(t, "postgres", gotID)
	assert.Equal(t, ConnSetting{ServiceType: "postgres", Conninfo: "conninfo"}, gotCS)

	_, _, err = ParsePostgresDSNEnv("INVALID", "conninfo")
	assert.Error(t, err)
}

func Test_ParsePgbouncerDSNEnv(t *testing.T) {
	gotID, gotCS, err := ParsePgbouncerDSNEnv("PGBOUNCER_DSN", "conninfo")
	assert.NoError(t, err)
	assert.Equal(t, "pgbouncer", gotID)
	assert.Equal(t, ConnSetting{ServiceType: "pgbouncer", Conninfo: "conninfo"}, gotCS)

	_, _, err = ParsePgbouncerDSNEnv("INVALID", "conninfo")
	assert.Error(t, err)
}

func Test_parseDSNEnv(t *testing.T) {
	testcases := []struct {
		valid    bool
		prefix   string
		key      string
		wantId   string
		wantType string
	}{
		{valid: true, prefix: "POSTGRES_DSN", key: "POSTGRES_DSN", wantId: "postgres", wantType: "postgres"},
		{valid: true, prefix: "POSTGRES_DSN", key: "POSTGRES_DSN_POSTGRES_123", wantId: "POSTGRES_123", wantType: "postgres"},
		{valid: true, prefix: "POSTGRES_DSN", key: "POSTGRES_DSN1", wantId: "1", wantType: "postgres"},
		{valid: true, prefix: "POSTGRES_DSN", key: "POSTGRES_DSN_POSTGRES_5432", wantId: "POSTGRES_5432", wantType: "postgres"},
		{valid: true, prefix: "PGBOUNCER_DSN", key: "PGBOUNCER_DSN", wantId: "pgbouncer", wantType: "pgbouncer"},
		{valid: true, prefix: "PGBOUNCER_DSN", key: "PGBOUNCER_DSN_PGBOUNCER_123", wantId: "PGBOUNCER_123", wantType: "pgbouncer"},
		{valid: true, prefix: "PGBOUNCER_DSN", key: "PGBOUNCER_DSN1", wantId: "1", wantType: "pgbouncer"},
		{valid: true, prefix: "PGBOUNCER_DSN", key: "PGBOUNCER_DSN_PGBOUNCER_6432", wantId: "PGBOUNCER_6432", wantType: "pgbouncer"},
		{valid: false, prefix: "POSTGRES_DSN", key: "POSTGRES_DSN_"},
		{valid: false, prefix: "POSTGRES_DSN", key: "INVALID"},
		{valid: false, prefix: "INVALID", key: "INVALID"},
	}

	for _, tc := range testcases {
		gotID, gotCS, err := parseDSNEnv(tc.prefix, tc.key, "conninfo")
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.wantId, gotID)
			assert.Equal(t, ConnSetting{ServiceType: tc.wantType, Conninfo: "conninfo"}, gotCS)
		} else {
			assert.Error(t, err)
		}
	}
}
