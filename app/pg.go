package app

import (
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"github.com/rs/zerolog/log"
	"pgscv/app/model"
)

const (
	dbDriver = "postgres"

	//errCodeInvalidPassword = "28P01"

	// PQhostQuery identifies an address of used connection to Postgres, is it a network address or UNIX socket
	PQhostQuery = "SELECT inet_server_addr() inet, current_setting('unix_socket_directories') unix;"
	// PQportQuery identifies a port number of used connection to Postgres
	PQportQuery = "SELECT coalesce(inet_server_port(),5432)"
	// PQuserQuery identifies an user which used to connect to Postgres
	PQuserQuery = "SELECT current_user"
	// PQdbQuery identifies a database to which made the connection
	PQdbQuery = "SELECT current_database()"
	// PQstatusQueryPostgres is query used for checking status of the Postgres's connection
	PQstatusQueryPostgres = "SELECT 1"
	// PQstatusQueryBouncer is query used for checking status of the Pgbouncer's connection
	PQstatusQueryBouncer = "SHOW VERSION"

	// TODO: если мы переопределяем сессионные переменные, то получаем их когда запрашиваем настройки pg_show_all_settings()

	// WorkMemQuery specfies work_mem value for working session
	WorkMemQuery = "SET work_mem TO '32MB'"
	// LogMinDurationQuery specifies SQL to override log_min_duration_statement
	LogMinDurationQuery = "SET log_min_duration_statement TO 10000"
	// StatementTimeoutQuery specifies SQL to override statement_timeout
	StatementTimeoutQuery = "SET statement_timeout TO 5000"
	// LockTimeoutQuery specifies SQL to override lock_timeout
	LockTimeoutQuery = "SET lock_timeout TO 2000"
	// DeadlockTimeoutQuery specifies SQL to override deadlock_timeout
	DeadlockTimeoutQuery = "SET deadlock_timeout TO 1000"
	// DbListQuery queries a list of connectable databases
	DbListQuery = "SELECT datname FROM pg_database WHERE NOT datistemplate AND datallowconn"
)

// CreateConn assembles 'libpq' connection string, connects to Postgres and returns 'connection' object
func CreateConn(c *model.Service) (conn *sql.DB, err error) {
	if c.ServiceType != model.ServiceTypePostgresql && c.ServiceType != model.ServiceTypePgbouncer {
		return nil, nil
	}

	// Assemble libpq-style connection string
	connstr := assembleConnstr(c)
	// Connect to Postgres using assembled connection string
	if conn, err = PQconnectdb(c, connstr); err != nil {
		return nil, err
	}

	// Check connection accepts commands
	if err := PQstatus(conn, c.ServiceType); err != nil {
		return nil, err
	}

	// Fill empty settings by normal values
	if err = replaceEmptySettings(c, conn); err != nil {
		return nil, err
	}

	// Set session's safe settings for PostgreSQL conns
	if c.ServiceType == model.ServiceTypePostgresql {
		setSafeSession(conn)
	}

	return conn, nil
}

// Build connection string using connection settings
func assembleConnstr(c *model.Service) string {
	s := fmt.Sprintf("sslmode=disable application_name=%s ", c.User)
	if c.Host != "" {
		s = fmt.Sprintf("%s host=%s ", s, c.Host)
	}
	if c.Port != 0 {
		s = fmt.Sprintf("%s port=%d ", s, c.Port)
	}
	if c.User != "" {
		s = fmt.Sprintf("%s user=%s ", s, c.User)
	}
	if c.Password != "" {
		s = fmt.Sprintf("%s password=%s ", s, c.Password)
	}
	if c.Dbname != "" {
		s = fmt.Sprintf("%s dbname=%s ", s, c.Dbname)
	}
	return s
}

// PQconnectdb connects to Postgres
func PQconnectdb(c *model.Service, connstr string) (conn *sql.DB, err error) {
	conn, err = sql.Open(dbDriver, connstr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Fill empty connection settings by normal values.
func replaceEmptySettings(c *model.Service, conn *sql.DB) (err error) {
	if c.Host == "" {
		c.Host, err = PQhost(conn)
		if err != nil {
			return err
		}
	}
	if c.Port == 0 {
		c.Port, _ = PQport(conn)
	}
	if c.User == "" {
		c.User, _ = PQuser(conn)
	}
	if c.Dbname == "" {
		c.Dbname, _ = PQdb(conn)
	}
	return nil
}

// Set session's safe settings. It's possible to pass these parameters via connection string at startup, but they're not logged then.
func setSafeSession(conn *sql.DB) {
	for _, query := range []string{WorkMemQuery, StatementTimeoutQuery, LockTimeoutQuery, DeadlockTimeoutQuery, LogMinDurationQuery} {
		_, err := conn.Exec(query)
		// Trying to SET superuser-only parameters for NOSUPERUSER will lead to error, but it's not critical.
		// Notice about occurred error, clear it and go on.
		if err, ok := err.(*pq.Error); ok {
			log.Warn().Msgf("%s: %s\nSTATEMENT: %s\n", err.Severity, err.Message, query)
		}
	}
}

// getDBList returns the list of databases that allowed for connection
func getDBList(conn *sql.DB) (list []string, err error) {
	rows, err := conn.Query(DbListQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list = make([]string, 0, 10)
	for rows.Next() {
		var dbname string
		if err := rows.Scan(&dbname); err != nil {
			return nil, err
		}
		list = append(list, dbname)
	}
	return list, nil
}

// PQhost returns endpoint (network address or socket directory) to which pgSCV is connected
func PQhost(c *sql.DB) (_ string, err error) {
	// Don't use query with coalesce(inet, unix), because 'inet' and 'unix' are the different types,
	// at casting 'inet' to text, netmask is added to the final address and the netmask is unnecessary.
	var i, u sql.NullString
	err = c.QueryRow(PQhostQuery).Scan(&i, &u)
	if err != nil {
		return "", err
	}
	if i.String != "" {
		return i.String, err
	}
	return u.String, err
}

// PQport returns the port number to which pgSCV is connected
func PQport(c *sql.DB) (i uint16, err error) {
	err = c.QueryRow(PQportQuery).Scan(&i)
	return i, err
}

// PQuser returns username which is used by pgSCV
func PQuser(c *sql.DB) (s string, err error) {
	err = c.QueryRow(PQuserQuery).Scan(&s)
	return s, err
}

// PQdb returns database name to which pgSCV is connected
func PQdb(c *sql.DB) (s string, err error) {
	err = c.QueryRow(PQdbQuery).Scan(&s)
	return s, err
}

// PQstatus returns connections status - just do 'SELECT 1' and return result - nil or err
func PQstatus(c *sql.DB, itype int) error {
	var q string

	switch itype {
	case model.ServiceTypePostgresql:
		q = PQstatusQueryPostgres
	case model.ServiceTypePgbouncer:
		q = PQstatusQueryBouncer
	}

	_, err := c.Exec(q)
	return err
}
