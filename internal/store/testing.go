package store

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

const testPostgresConnStr = "host=postgres dbname=pgscv_fixtures user=postgres sslmode=disable"

func NewTest(t *testing.T) *DB {
	db, err := New(testPostgresConnStr)
	assert.NoError(t, err)
	return db
}
