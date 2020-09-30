package store

import (
	"database/sql"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNew(t *testing.T) {
	var testcases = []struct {
		dsn   string
		valid bool
	}{
		{dsn: TestPostgresConnStr, valid: true},
		{dsn: "host=127.0.0.1 dbname=pgscv_fixtures user=pgscv sslmode=require", valid: true},
		{dsn: "host=127.0.0.1 dbname=invalid user=pgscv sslmode=disable", valid: false},
		{dsn: "invalid_string", valid: false},
	}

	for _, tc := range testcases {
		db, err := New(tc.dsn)
		if tc.valid {
			assert.NoError(t, err)
			assert.NotNil(t, db)
		} else {
			assert.Error(t, err)
			assert.Nil(t, db)
		}
	}
}

func TestNewWithConfig(t *testing.T) {
	var testcases = []struct {
		valid    bool
		database string
	}{
		{valid: true, database: "pgscv_fixtures"},
		{valid: false, database: "__invalid__"},
	}

	for _, tc := range testcases {
		config, err := pgx.ParseConfig(TestPostgresConnStr)
		assert.NoError(t, err)

		config.Database = tc.database
		db, err := NewWithConfig(config)
		if tc.valid {
			assert.NoError(t, err)
			assert.NotNil(t, db)
		} else {
			assert.Error(t, err)
			assert.Nil(t, db)
		}
	}
}

func TestDB_Query(t *testing.T) {
	db := NewTest(t)

	testCases := []struct {
		name  string
		query string
		want  *model.PGResult
		valid bool
	}{
		{
			name:  "valid query",
			query: "SELECT 'example'||i AS example, i+1 AS one, i+2 AS two, i+3 AS three, i+4 AS four FROM generate_series(1,3) as gs(i)",
			want: &model.PGResult{
				Nrows: 3,
				Ncols: 5,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("example")}, {Name: []byte("one")}, {Name: []byte("two")}, {Name: []byte("three")}, {Name: []byte("four")},
				},
				Rows: [][]sql.NullString{
					{{String: "example1", Valid: true}, {String: "2", Valid: true}, {String: "3", Valid: true}, {String: "4", Valid: true}, {String: "5", Valid: true}},
					{{String: "example2", Valid: true}, {String: "3", Valid: true}, {String: "4", Valid: true}, {String: "5", Valid: true}, {String: "6", Valid: true}},
					{{String: "example3", Valid: true}, {String: "4", Valid: true}, {String: "5", Valid: true}, {String: "6", Valid: true}, {String: "7", Valid: true}},
				},
			},
			valid: true,
		},
		{
			name:  "invalid query",
			query: "invalid",
			valid: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(tc.query)

			if tc.valid {
				assert.NoError(t, err)
				assert.Equal(t, tc.want.Nrows, res.Nrows)
				assert.Equal(t, tc.want.Ncols, res.Ncols)
				assert.EqualValues(t, tc.want.Rows, res.Rows)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestDB_Close(t *testing.T) {
	db := NewTest(t)
	assert.NotNil(t, db)

	db.Close()
}
