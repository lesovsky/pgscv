package store_test

import (
	"github.com/barcodepro/pgscv/service/store"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewDB(t *testing.T) {
	var testcases = []struct {
		dsn   string
		valid bool
	}{
		{dsn: store.TestConnString(), valid: true},
		{dsn: "host=127.0.0.1 dbname=postgres user=postgres sslmode=require", valid: true},
		{dsn: "host=127.0.0.1 dbname=invalid user=postgres sslmode=disable", valid: false},
		{dsn: "invalid_string", valid: false},
	}

	for _, tc := range testcases {
		db, err := store.NewDB(tc.dsn)
		if tc.valid {
			assert.NoError(t, err)
			assert.NotNil(t, db)
		} else {
			assert.Error(t, err)
			assert.Nil(t, db)
		}
	}
}

func TestNewDBConfig(t *testing.T) {
	var testcases = []struct {
		valid    bool
		database string
	}{
		{valid: true, database: "postgres"},
		{valid: true, database: ""},
		{valid: false, database: "__invalid__"},
	}

	for _, tc := range testcases {
		config := store.TestConnConfig(t)
		config.Database = tc.database
		db, err := store.NewDBConfig(config)
		if tc.valid {
			assert.NoError(t, err)
			assert.NotNil(t, db)
		} else {
			assert.Error(t, err)
			assert.Nil(t, db)
		}
	}
}

func TestStore_Close(t *testing.T) {
	db, err := store.NewDB(store.TestConnString())
	assert.NoError(t, err)
	assert.NotNil(t, db)

	db.Close()
}
