package store

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

const TestPostgresConnStr = "host=127.0.0.1 dbname=pgscv_fixtures user=pgscv sslmode=disable"

func NewTest(t *testing.T) *DB {
	db, err := New(TestPostgresConnStr)
	assert.NoError(t, err)
	return db
}
