package store

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
)

const (
	queryDatabasesList = "SELECT datname FROM pg_database WHERE NOT datistemplate AND datallowconn"
)

// DB is the database representation
type DB struct {
	Config *pgx.ConnConfig // config used for connecting this database
	Conn   *pgx.Conn       // database connection object
}

// NewConn creates new connection to Postgres/Pgbouncer using passed DSN
func NewDB(connString string) (*DB, error) {
	config, err := pgx.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	// enable compatibility with pgbouncer
	config.PreferSimpleProtocol = true

	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}

	return &DB{Config: config, Conn: conn}, nil
}

// NewConnConfig creates new connection to Postgres/Pgbouncer using passed Config
func NewDBConfig(config *pgx.ConnConfig) (*DB, error) {
	// enable compatibility with pgbouncer
	config.PreferSimpleProtocol = true

	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}

	return &DB{Config: config, Conn: conn}, nil
}

func (db *DB) GetDatabases() ([]string, error) {
	// getDBList returns the list of databases that allowed for connection
	rows, err := db.Conn.Query(context.Background(), queryDatabasesList)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list = make([]string, 0, 10)
	for rows.Next() {
		var dbname string
		if err := rows.Scan(&dbname); err != nil {
			return nil, err
		}
		list = append(list, dbname)
	}
	return list, nil
}

// IsExtensionAvailable returns true if extension with specified name exists and available
func (db *DB) IsExtensionAvailable(name string) bool {
	log.Debugf("check %s availability", name)

	var (
		checkExtensionQuery = fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM information_schema.views WHERE table_name = '%s')", name)
		checkContentQuery   = fmt.Sprintf("SELECT 1 FROM %s LIMIT 1", name)
		exists              bool
		count               int
	)

	if err := db.Conn.QueryRow(context.Background(), checkExtensionQuery).Scan(&exists); err != nil {
		log.Errorln("failed to check extensions in information_schema: ", err)
		return false
	}

	// Return false if extension is not installed.
	if !exists {
		return false
	}

	// Execute simple query to check extension is queryable.
	if err := db.Conn.QueryRow(context.Background(), checkContentQuery).Scan(&count); err != nil {
		log.Errorf("%s exists but not queryable: ", err)
		return false
	}

	return true
}

// Close database connections gracefully
func (db *DB) Close() {
	err := db.Conn.Close(context.Background())
	if err != nil {
		log.Warnf("failed to close database connection: %s; ignore", err)
	}
}

// QueryResult is the iterable store that contains result of query - data (values) and metadata (number of rows, columns and names).
type QueryResult struct {
	Nrows    int
	Ncols    int
	Colnames []pgproto3.FieldDescription
	Rows     [][]sql.NullString
}

// GetStats executed query and wraps result into QueryResult struct
func (db *DB) GetStats(query string) (*QueryResult, error) {
	rows, err := db.Conn.Query(context.Background(), query)
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

	return &QueryResult{
		Nrows:    nrows,
		Ncols:    ncols,
		Colnames: colnames,
		Rows:     rowsStore,
	}, nil
}
