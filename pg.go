//
package main

import (
	"database/sql"
	"fmt"
	"github.com/lib/pq"
)

const (
	dbDriver = "postgres"

	errCodeInvalidPassword = "28P01"

	// Self-identification queries
	PQhostQuery           = "SELECT inet_server_addr() inet, current_setting('unix_socket_directories') unix;"
	PQportQuery           = "SELECT coalesce(inet_server_port(),5432)"
	PQuserQuery           = "SELECT current_user"
	PQdbQuery             = "SELECT current_database()"
	PQstatusQueryPostgres = "SELECT 1"
	PQstatusQueryBouncer  = "SHOW VERSION"

	// Session parameters used by pgCenter at connection start
	WorkMemQuery          = "SET work_mem TO '32MB'"
	LogMinDurationQuery   = "SET log_min_duration_statement TO 10000"
	StatementTimeoutQuery = "SET statement_timeout TO 5000"
	LockTimeoutQuery      = "SET lock_timeout TO 2000"
	DeadlockTimeoutQuery  = "SET deadlock_timeout TO 1000"

	DbListQuery = "SELECT datname FROM pg_database WHERE NOT datistemplate AND datallowconn"
)

// Assembles libpq connection string, connect to Postgres and returns 'connection' object
func CreateConn(c *Instance) (conn *sql.DB, err error) {
	if c.InstanceType >= STYPE_SYSTEM {
		return nil, nil
	}

	// Assemble libpq-style connection string
	connstr := assembleConnstr(c)
	// Connect to Postgres using assembled connection string
	if conn, err = PQconnectdb(c, connstr); err != nil {
		return nil, err
	}

	// Check connection accepts commands
	if err := PQstatus(conn, c.InstanceType); err != nil {
		return nil, err
	}

	// Fill empty settings by normal values
	if err = replaceEmptySettings(c, conn); err != nil {
		return nil, err
	}

	// Set session's safe settings for PostgreSQL conns
	if c.InstanceType == STYPE_POSTGRESQL {
		setSafeSession(conn)
	}

	return conn, nil
}

// Build connection string using connection settings
func assembleConnstr(c *Instance) string {
	s := "sslmode=disable application_name=pgscv "
	if c.Host != "" {
		s = fmt.Sprintf("%s host=%s ", s, c.Host)
	}
	if c.Port != 0 {
		s = fmt.Sprintf("%s port=%d ", s, c.Port)
	}
	if c.User != "" {
		s = fmt.Sprintf("%s user=%s ", s, c.User)
	}
	if c.Dbname != "" {
		s = fmt.Sprintf("%s dbname=%s ", s, c.Dbname)
	}
	return s
}

// Connect to Postgres, ask password if required.
func PQconnectdb(c *Instance, connstr string) (conn *sql.DB, err error) {
	conn, err = sql.Open(dbDriver, connstr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Fill empty connection settings by normal values.
func replaceEmptySettings(c *Instance, conn *sql.DB) (err error) {
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

// Set session's safe settings.
// It's possible to pass these parameters via connection string at startup, but they're not logged then.
func setSafeSession(conn *sql.DB) {
	for _, query := range []string{WorkMemQuery, StatementTimeoutQuery, LockTimeoutQuery, DeadlockTimeoutQuery, LogMinDurationQuery} {
		_, err := conn.Exec(query)
		// Trying to SET superuser-only parameters for NOSUPERUSER will lead to error, but it's not critical.
		// Notice about occured error, clear it and go on.
		if err, ok := err.(*pq.Error); ok {
			fmt.Printf("%s: %s\nSTATEMENT: %s\n", err.Severity, err.Message, query)
		}
		err = nil
	}
}

//
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

// Returns endpoint (hostname or UNIX-socket) to which pgCenter is connected
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

// Returns port number to which pgCenter is connected
func PQport(c *sql.DB) (i int, err error) {
	err = c.QueryRow(PQportQuery).Scan(&i)
	return i, err
}

// Returns username which is used by pgCenter
func PQuser(c *sql.DB) (s string, err error) {
	err = c.QueryRow(PQuserQuery).Scan(&s)
	return s, err
}

// Returns database name to which pgCenter is connected
func PQdb(c *sql.DB) (s string, err error) {
	err = c.QueryRow(PQdbQuery).Scan(&s)
	return s, err
}

// Returns connections status
func PQstatus(c *sql.DB, itype int) (error) {
	var q string

	switch itype {
	case STYPE_POSTGRESQL:
		q = PQstatusQueryPostgres
	case STYPE_PGBOUNCER:
		q = PQstatusQueryBouncer
	}

	_, err := c.Exec(q)
	return err
}
