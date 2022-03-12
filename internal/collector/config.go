package collector

import (
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/lesovsky/pgscv/internal/log"
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/lesovsky/pgscv/internal/store"
	"net"
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
	// BaseURL defines a URL string for connecting to HTTP service
	BaseURL string
	// NoTrackMode controls collector to gather and send sensitive information, such as queries texts.
	NoTrackMode bool
	// postgresServiceConfig defines collector's options specific for Postgres service
	postgresServiceConfig
	// DatabasesRE defines regexp with databases from which builtin metrics should be collected.
	DatabasesRE *regexp.Regexp
	// Settings defines collectors settings propagated from main YAML configuration.
	Settings model.CollectorsSettings
}

// postgresServiceConfig defines Postgres-specific stuff required during collecting Postgres metrics.
type postgresServiceConfig struct {
	// localService defines service is running on the local host.
	localService bool
	// blockSize defines size of data block Postgres operates.
	blockSize uint64
	// walSegmentSize defines size of WAL segment Postgres operates.
	walSegmentSize uint64
	// serverVersionNum defines version of Postgres in XXYYZZ format.
	serverVersionNum int
	// dataDirectory defines filesystem path where Postgres' data files and directories resides.
	dataDirectory string
	// loggingCollector defines value of 'logging_collector' GUC.
	loggingCollector bool
	// pgStatStatements defines is pg_stat_statements available in shared_preload_libraries and available for queries
	pgStatStatements bool
	// pgStatStatementsDatabase defines the database name where pg_stat_statements is available
	pgStatStatementsDatabase string
	// pgStatStatementsSchema defines the schema name where pg_stat_statements is installed
	pgStatStatementsSchema string
}

// newPostgresServiceConfig defines new config for Postgres-based collectors.
func newPostgresServiceConfig(connStr string) (postgresServiceConfig, error) {
	var config = postgresServiceConfig{}

	// Return empty config if empty connection string.
	if connStr == "" {
		return config, nil
	}

	pgconfig, err := pgx.ParseConfig(connStr)
	if err != nil {
		return config, err
	}

	// Determine is service running locally.
	config.localService = isAddressLocal(pgconfig.Host)

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

	config.blockSize = bsize

	// Get Postgres WAL segment size.
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'wal_segment_size'").Scan(&setting)
	if err != nil {
		return config, err
	}
	walSegSize, err := strconv.ParseUint(setting, 10, 64)
	if err != nil {
		return config, err
	}

	config.walSegmentSize = walSegSize

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

	config.serverVersionNum = version

	// Get Postgres data directory
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'data_directory'").Scan(&setting)
	if err != nil {
		return config, err
	}

	config.dataDirectory = setting

	// Get setting of 'logging_collector' GUC.
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'logging_collector'").Scan(&setting)
	if err != nil {
		return config, err
	}

	if setting == "on" {
		config.loggingCollector = true
	}

	// Discover pg_stat_statements.
	exists, database, schema, err := discoverPgStatStatements(connStr)
	if err != nil {
		return config, err
	}

	if !exists {
		log.Warnln("pg_stat_statements not found, skip collecting statements metrics")
	}

	config.pgStatStatements = exists
	config.pgStatStatementsDatabase = database
	config.pgStatStatementsSchema = schema

	return config, nil
}

// isAddressLocal return true if passed address is local, and return false otherwise.
func isAddressLocal(addr string) bool {
	if addr == "" {
		return false
	}

	if strings.HasPrefix(addr, "/") {
		return true
	}

	if addr == "localhost" || strings.HasPrefix(addr, "127.") || addr == "::1" {
		return true
	}

	addresses, err := net.InterfaceAddrs()
	if err != nil {
		// Consider error as the passed host address is not local
		log.Warnf("check network address '%s' failed: %s; consider it as remote", addr, err)
		return false
	}

	for _, a := range addresses {
		if strings.HasPrefix(a.String(), addr) {
			return true
		}
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
