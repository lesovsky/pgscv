package store

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/lesovsky/pgscv/internal/log"
	"github.com/lesovsky/pgscv/internal/model"
)

const (
	// Data types supported by parser of query results.
	dataTypeBool uint32 = 16
	// dataTypeChar uint32 = 18 is not supported - its conversion to sql.NullString lead to panic 'pgx' driver.
	dataTypeName    uint32 = 19
	dataTypeInt8    uint32 = 20
	dataTypeInt2    uint32 = 21
	dataTypeInt4    uint32 = 23
	dataTypeText    uint32 = 25
	dataTypeOid     uint32 = 26
	dataTypeFloat4  uint32 = 700
	dataTypeFloat8  uint32 = 701
	dataTypeInet    uint32 = 869
	dataTypeBpchar  uint32 = 1042
	dataTypeVarchar uint32 = 1043
	dataTypeNumeric uint32 = 1700
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

	// Using simple protocol requires explicit options to be set.
	config.RuntimeParams = map[string]string{
		"standard_conforming_strings": "on",
		"client_encoding":             "UTF8",
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

	// Not the all data types could be safely converted into sql.NullString
	// and conversion errors lead to panic.
	// Check the data types are safe in returned result.
	for _, c := range colnames {
		if !isDataTypeSupported(c.DataTypeOID) {
			return nil, fmt.Errorf("query '%s', unsupported data type OID: %d", query, c.DataTypeOID)
		}
	}

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

// isDataTypeSupported tests passed type OID is supported.
func isDataTypeSupported(t uint32) bool {
	switch t {
	case dataTypeName, dataTypeBpchar, dataTypeVarchar, dataTypeText,
		dataTypeInt2, dataTypeInt4, dataTypeInt8,
		dataTypeOid, dataTypeFloat4, dataTypeFloat8, dataTypeNumeric,
		dataTypeBool, dataTypeInet:
		return true
	default:
		return false
	}
}
