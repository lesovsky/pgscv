package collector

import (
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"regexp"
	"strconv"
	"strings"
)

// Config defines collector's global configuration.
type Config struct {
	// ServiceType defines the type of discovered service. Depending on the type there should be different settings or
	// settings-specifics metric collection usecases.
	ServiceType string
	// ConnString defines a connection string used to connecting to the service
	ConnString string
	// NoTrackMode controls collector to gather and send sensitive information, such as queries texts.
	NoTrackMode bool
	// PostgresServiceConfig defines collector's options specific for Postgres service
	PostgresServiceConfig
	// DatabasesRE defines regexp with databases from which builtin metrics should be collected.
	DatabasesRE *regexp.Regexp
	// Settings defines collectors settings propagated from main YAML configuration.
	Settings model.CollectorsSettings
}

// PostgresServiceConfig defines Postgres-specific stuff required during collecting Postgres metrics.
// TODO: make struct and fields private
type PostgresServiceConfig struct {
	// LocalService defines service is running on the local host.
	LocalService bool
	// BlockSize defines size of data block Postgres operates.
	BlockSize uint64
	// WalSegmentSize defines size of WAL segment Postgres operates.
	WalSegmentSize uint64
	// ServerVersionNum defines version of Postgres in XXYYZZ format.
	ServerVersionNum int
	// DataDirectory defines filesystem path where Postgres' data files and directories resides.
	DataDirectory string
	// LoggingCollector defines value of 'logging_collector' GUC.
	LoggingCollector bool
	// PgStatStatements defines is pg_stat_statements available in shared_preload_libraries and available for queries
	PgStatStatements bool
	// PgStatStatementsDatabase defines the database name where pg_stat_statements is available
	PgStatStatementsDatabase string
	// PgStatStatementsSchema defines the schema name where pg_stat_statements is installed
	PgStatStatementsSchema string
}

// newPostgresServiceConfig defines new config for Postgres-based collectors.
func newPostgresServiceConfig(connStr string) (PostgresServiceConfig, error) {
	var config = PostgresServiceConfig{}

	// Return empty config if empty connection string.
	if connStr == "" {
		return config, nil
	}

	pgconfig, err := pgx.ParseConfig(connStr)
	if err != nil {
		return config, err
	}

	// Determine is service running locally.
	config.LocalService = isAddressLocal(pgconfig.Host)

	conn, err := store.NewWithConfig(pgconfig)
	if err != nil {
		return config, err
	}
	defer conn.Close()

	var setting string

	// Get Postgres block size.
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'block_size'").Scan(&setting)
	if err != nil {
		return config, err
	}
	bsize, err := strconv.ParseUint(setting, 10, 64)
	if err != nil {
		return config, err
	}

	config.BlockSize = bsize

	// Get Postgres WAL segment size.
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'wal_segment_size'").Scan(&setting)
	if err != nil {
		return config, err
	}
	segSize, err := strconv.ParseUint(setting, 10, 64)
	if err != nil {
		return config, err
	}

	config.WalSegmentSize = segSize

	// Get Postgres server version
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'server_version_num'").Scan(&setting)
	if err != nil {
		return config, err
	}
	version, err := strconv.Atoi(setting)
	if err != nil {
		return config, err
	}

	if version < PostgresVMinNum {
		log.Warnf("Postgres version is too old, some collectors functions won't work. Minimal required version is %s.", PostgresVMinStr)
	}

	config.ServerVersionNum = version

	// Get Postgres data directory
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'data_directory'").Scan(&setting)
	if err != nil {
		return config, err
	}

	config.DataDirectory = setting

	// Get setting of 'logging_collector' GUC.
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'logging_collector'").Scan(&setting)
	if err != nil {
		return config, err
	}

	if setting == "on" {
		config.LoggingCollector = true
	}

	// Discover pg_stat_statements.
	exists, database, schema, err := discoverPgStatStatements(connStr)
	if err != nil {
		return config, err
	}

	if !exists {
		log.Info("pg_stat_statements is not found in shared_preload_libraries, disable pg_stat_statements metrics collection")
		config.PgStatStatements = false
	}

	config.PgStatStatements = true
	config.PgStatStatementsDatabase = database
	config.PgStatStatementsSchema = schema

	return config, nil
}

// isAddressLocal return true if passed address is local, and return false otherwise.
func isAddressLocal(addr string) bool {
	if strings.HasPrefix(addr, "/") || strings.HasPrefix(addr, "127.") || addr == "localhost" {
		return true
	}
	return false
}

// discoverPgStatStatements discovers pg_stat_statements, what database and schema it is installed.
func discoverPgStatStatements(connStr string) (bool, string, string, error) {
	pgconfig, err := pgx.ParseConfig(connStr)
	if err != nil {
		return false, "", "", err
	}

	conn, err := store.NewWithConfig(pgconfig)
	if err != nil {
		return false, "", "", err
	}

	var setting string
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'shared_preload_libraries'").Scan(&setting)
	if err != nil {
		return false, "", "", err
	}

	// If pg_stat_statements is not enabled globally, no reason to continue.
	if !strings.Contains(setting, "pg_stat_statements") {
		conn.Close()
		return false, "", "", nil
	}

	// Check for pg_stat_statements in default database specified in connection string.
	if schema := extensionInstalledSchema(conn, "pg_stat_statements"); schema != "" {
		conn.Close()
		return true, conn.Conn().Config().Database, schema, nil
	}

	// Pessimistic case.
	// If we're here it means pg_stat_statements is not available
	// and we have to walk through all database and looking for it.

	// Get databases list from current connection.
	databases, err := listDatabases(conn)
	if err != nil {
		conn.Close()
		return false, "", "", err
	}

	// Close connection to current database, it's not interesting anymore.
	conn.Close()

	// Establish connection to each database in the list and check where pg_stat_statements is installed.
	for _, d := range databases {
		pgconfig.Database = d
		conn, err := store.NewWithConfig(pgconfig)
		if err != nil {
			log.Warnf("connect to database '%s' failed: %s; skip", pgconfig.Database, err)
			continue
		}

		// If pg_stat_statements found, update source and return connection.
		if schema := extensionInstalledSchema(conn, "pg_stat_statements"); schema != "" {
			return true, conn.Conn().Config().Database, schema, nil
		}

		// Otherwise close connection and go to next database in the list.
		conn.Close()
	}

	// No luck.
	// If we are here it means all database checked and
	// pg_stat_statements is not found (not installed).
	return false, "", "", nil
}

// extensionInstalledSchema returns schema name where extension is installed, or empty if not installed.
func extensionInstalledSchema(db *store.DB, name string) string {
	log.Debugf("check %s extension availability", name)

	var schema string
	err := db.Conn().
		QueryRow(context.Background(), "SELECT extnamespace::regnamespace FROM pg_extension WHERE extname = $1", name).
		Scan(&schema)
	if err != nil && err != pgx.ErrNoRows {
		log.Errorf("failed to check extensions '%s' in pg_extension: %s", name, err)
		return ""
	}

	return schema
}
