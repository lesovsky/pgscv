package collector

import (
	"context"
	"github.com/barcodepro/pgscv/internal/filter"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/jackc/pgx/v4"
	"strconv"
	"strings"
)

// Config defines collector configuration settings
type Config struct {
	// ServiceType defines the type of discovered service. Depending on the type there should be different settings or
	// settings-specifics metric collection usecases.
	ServiceType string
	// ConnString defines a connection string used to connecting to the service
	ConnString string
	// AllowTrackSensitive controls collector to gather and send sensitive information, such as queries texts.
	AllowTrackSensitive bool
	// PostgresServiceConfig defines collector's options specific for Postgres service
	PostgresServiceConfig
	// Filters are user-defined regular expressions allow to include/exclude collecting various stats.
	Filters map[string]filter.Filter
}

// PostgresServiceConfig defines Postgres-specific stuff required during collecting Postgres metrics.
type PostgresServiceConfig struct {
	// ServerVersionNum defines version of Postgres in XXYYZZ format.
	ServerVersionNum int
	// DataDirectory defines filesystem path where Postgres' data files and directories resides.
	DataDirectory string
	// PgStatStatements defines is pg_stat_statements available in shared_preload_libraries and available for queries
	PgStatStatements bool
	// PgStatStatementsSource defines the database name where pg_stat_statements is available
	PgStatStatementsSource string
}

// NewPostgresServiceConfig defines new config for Postgres-based collectors
func NewPostgresServiceConfig(connStr string) (PostgresServiceConfig, error) {
	var config = PostgresServiceConfig{
		PgStatStatements:       false,
		PgStatStatementsSource: "",
	}

	pgconfig, err := pgx.ParseConfig(connStr)
	if err != nil {
		return config, err
	}

	conn, err := store.NewWithConfig(pgconfig)
	if err != nil {
		return config, err
	}
	defer conn.Close()

	var setting string

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
		log.Warnf("Postgres version is too old, some collectors functions won't work. Minimum required version is %s.", PostgresVMinStr)
	}

	config.ServerVersionNum = version

	// Get Postgres data directory
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'data_directory'").Scan(&setting)
	if err != nil {
		return config, err
	}

	config.DataDirectory = setting

	// Get shared_preload_libraries (for inspecting enabled extensions).
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'shared_preload_libraries'").Scan(&setting)
	if err != nil {
		return config, err
	}
	if !strings.Contains(setting, "pg_stat_statements") {
		log.Info("pg_stat_statements is not found in shared_preload_libraries, disable pg_stat_statements metrics collection")
	}

	// Enable PgStatStatements, but leave empty PgStatStatementsSource, it will be filled at first execution of collector's Update method.
	config.PgStatStatements = true

	return config, nil
}
