package store

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

const testPostgresConnStr = "host=127.0.0.1 dbname=postgres user=postgres sslmode=disable"

func NewTest(t *testing.T) *DB {
	db, err := New(testPostgresConnStr)
	assert.NoError(t, err)
	return db
}
