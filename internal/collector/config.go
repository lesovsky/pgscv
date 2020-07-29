package collector

import (
	"context"
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
}

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
// TODO: maybe we need return error too, and abort adding service if creating config failed ?
func NewPostgresServiceConfig(connStr string) PostgresServiceConfig {
	var config = PostgresServiceConfig{
		PgStatStatements:       false,
		PgStatStatementsSource: "",
	}

	pgconfig, err := pgx.ParseConfig(connStr)
	if err != nil {
		log.Errorln("failed create new PostgresServiceConfig: ", err)
		return config
	}

	conn, err := store.NewWithConfig(pgconfig)
	if err != nil {
		log.Errorln("failed create new PostgresServiceConfig: ", err)
		return config
	}
	defer conn.Close()

	var setting string

	// Get Postgres server version
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'server_version_num'").Scan(&setting)
	if err != nil {
		log.Errorln("failed read server_version_num setting: ", err)
		return config
	}
	version, err := strconv.Atoi(setting)
	if err != nil {
		log.Errorln("failed convert server_version_num to int: ", err)
		return config
	}

	config.ServerVersionNum = version

	// Get Postgres data directory
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'data_directory'").Scan(&setting)
	if err != nil {
		log.Errorln("failed read data_directory setting: ", err)
		return config
	}

	config.DataDirectory = setting

	// Get shared_preload_libraries (for inspecting enabled extensions).
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'shared_preload_libraries'").Scan(&setting)
	if err != nil {
		log.Errorln("failed read shared_preload_libraries setting: ", err)
		return config
	}
	if !strings.Contains(setting, "pg_stat_statements") {
		log.Info("pg_stat_statements is not found in shared_preload_libraries, disable pg_stat_statements metrics collection")
	}

	config.PgStatStatements = true // leave PgStatStatementsSource empty, it will be filled in first execution of collector's Update method

	return config
}
