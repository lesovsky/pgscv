package model

import (
	"database/sql"
	"github.com/jackc/pgproto3/v2"
)

const (
	// Pull mode is the classic mode recommended by Prometheus - exporter listens for scrapes made by remote system.
	RuntimePullMode int = 1
	// Push mode is the old-style mode when exporter push collected metrics into remote system.
	RuntimePushMode int = 2

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
