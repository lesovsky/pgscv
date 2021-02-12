package store

import (
	"context"
	"database/sql"
	"github.com/jackc/pgx/v4"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
)

// DB is the database representation
type DB struct {
	conn *pgx.Conn // database connection object
}

// New creates new connection to Postgres/Pgbouncer using passed DSN
func New(connString string) (*DB, error) {
	config, err := pgx.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	return NewWithConfig(config)
}

// NewWithConfig creates new connection to Postgres/Pgbouncer using passed Config.
func NewWithConfig(config *pgx.ConnConfig) (*DB, error) {
	// Enable simple protocol for compatibility with Pgbouncer.
	config.PreferSimpleProtocol = true

	// Explicitly set standard_conforming_strings to 'on' which is required when using simple protocol.
	config.RuntimeParams = map[string]string{
		"standard_conforming_strings": "on",
	}

	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}

	return &DB{conn: conn}, nil
}

/* public db methods */

// Query is a wrapper on private query() method.
func (db *DB) Query(query string) (*model.PGResult, error) { return db.query(query) }

// Close is wrapper on private close() method.
func (db *DB) Close() { db.close() }

// Conn provides access to public methods of *pgx.Conn struct
func (db *DB) Conn() *pgx.Conn { return db.conn }

/* private db methods */

// Query method executes passed query and wraps result into model.PGResult struct.
func (db *DB) query(query string) (*model.PGResult, error) {
	rows, err := db.Conn().Query(context.Background(), query)
	if err != nil {
		return nil, err
	}

	// Generic variables describe properties of query result.
	var (
		colnames = rows.FieldDescriptions()
		ncols    = len(colnames)
		nrows    int
	)

	// Storage used for data extracted from rows.
	// Scan operation supports only slice of interfaces, 'pointers' slice is the intermediate store where all values written.
	// Next values from 'pointers' associated with type-strict slice - 'values'. When Scan is writing to the 'pointers' it
	// also writing to the 'values' under the hood. When all pointers/values have been scanned, put them into 'rowsStore'.
	// Finally we get queryResult iterable store with data and information about stored rows, columns and columns names.
	var rowsStore = make([][]sql.NullString, 0, 10)

	for rows.Next() {
		pointers := make([]interface{}, ncols)
		values := make([]sql.NullString, ncols)

		for i := range pointers {
			pointers[i] = &values[i]
		}

		err = rows.Scan(pointers...)
		if err != nil {
			log.Warnf("skip collecting stats: %s", err)
			continue
		}
		rowsStore = append(rowsStore, values)
		nrows++
	}

	rows.Close()

	return &model.PGResult{
		Nrows:    nrows,
		Ncols:    ncols,
		Colnames: colnames,
		Rows:     rowsStore,
	}, nil
}

// Close method closes database connections gracefully.
func (db *DB) close() {
	err := db.Conn().Close(context.Background())
	if err != nil {
		log.Warnf("failed to close database connection: %s; ignore", err)
	}
}
