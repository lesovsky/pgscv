package store

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

// TestConnString returns test database connection string
func TestConnString() string {
	return "host=127.0.0.1 dbname=postgres user=postgres sslmode=disable"
}

// TestConnConfig return test database connection config
func TestConnConfig(t *testing.T) *pgx.ConnConfig {
	config, err := pgx.ParseConfig(TestConnString())
	assert.NoError(t, err)
	assert.NotNil(t, config)
	return config
}

// TestDB provides store for testing with teardown function which allow to cleanup store after tests
func TestDB(t *testing.T, connString string) (*DB, func(...string)) {
	t.Helper()
	var s = &DB{}

	assert.NotEmpty(t, connString)

	db, err := NewDB(connString)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	return s, func(tables ...string) {
		if len(tables) > 0 {
			if _, err := db.Conn.Exec(context.Background(), fmt.Sprintf("TRUNCATE %s CASCADE", strings.Join(tables, ","))); err != nil {
				t.Fatal(err)
			}
		}
		s.Close()
	}
}
