package model

import (
	"database/sql"
	"github.com/jackc/pgproto3/v2"
)

const (
	// Service label string for system service
	ServiceTypeSystem = "system"
	// Service label string for Postgres services
	ServiceTypePostgresql = "postgres"
	// Service label string for Pgbouncer services
	ServiceTypePgbouncer = "pgbouncer"
)

// PGResult is the iterable store that contains query result (data and metadata) returned from Postgres
type PGResult struct {
	Nrows    int
	Ncols    int
	Colnames []pgproto3.FieldDescription
	Rows     [][]sql.NullString
}
