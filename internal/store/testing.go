package store

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

const TestPostgresConnStr = "host=127.0.0.1 port=5432 user=pgscv dbname=pgscv_fixtures sslmode=disable"
const TestPgbouncerConnStr = "host=127.0.0.1 port=6432 user=pgscv dbname=pgbouncer sslmode=disable"

func NewTest(t *testing.T) *DB {
	db, err := New(TestPostgresConnStr)
	assert.NoError(t, err)
	return db
}

func NewTestPgbouncer(t *testing.T) *DB {
	db, err := New(TestPgbouncerConnStr)
	assert.NoError(t, err)
	return db
}
