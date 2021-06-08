package collector

import (
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/store"
	"testing"
)

func TestNewPostgresServiceConfig(t *testing.T) {
	var testCases = []struct {
		name    string
		connStr string
		valid   bool
	}{
		{name: "valid config", connStr: "host=127.0.0.1 dbname=pgscv_fixtures user=pgscv", valid: true},
		{name: "invalid config", connStr: "invalid", valid: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewPostgresServiceConfig(tc.connStr)
			if tc.valid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func Test_isAddressLocal(t *testing.T) {
	testcases := []struct {
		addr string
		want bool
	}{
		{addr: "127.0.0.1", want: true},
		{addr: "127.1.2.3", want: true},
		{addr: "/var/run/postgresql", want: true},
		{addr: "localhost", want: true},
		{addr: "", want: false},
		{addr: "1.2.3.4", want: false},
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.want, isAddressLocal(tc.addr))
	}
}

func Test_discoverPgStatStatements(t *testing.T) {
	testcases := []struct {
		valid   bool
		connstr string
	}{
		{valid: true, connstr: store.TestPostgresConnStr},
		{valid: false, connstr: "database"},
		{valid: false, connstr: "database=invalid"},
	}

	for _, tc := range testcases {
		exists, database, schema, err := discoverPgStatStatements(tc.connstr)
		if tc.valid {
			assert.True(t, exists)
			assert.Equal(t, "pgscv_fixtures", database)
			assert.Equal(t, "public", schema)
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_extensionInstalledSchema(t *testing.T) {
	conn := store.NewTest(t)

	assert.Equal(t, extensionInstalledSchema(conn, "plpgsql"), "pg_catalog")
	assert.Equal(t, extensionInstalledSchema(conn, "invalid"), "")
	conn.Close()
}
