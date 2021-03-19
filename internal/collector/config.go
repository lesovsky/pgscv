package collector

import (
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/weaponry/pgscv/internal/filter"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/store"
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
	// NoTrackMode controls collector to gather and send sensitive information, such as queries texts.
	NoTrackMode bool
	// PostgresServiceConfig defines collector's options specific for Postgres service
	PostgresServiceConfig
	// Filters are user-defined regular expressions allow to include/exclude collecting various stats.
	Filters map[string]filter.Filter
}

// PostgresServiceConfig defines Postgres-specific stuff required during collecting Postgres metrics.
type PostgresServiceConfig struct {
	// TODO: cast type to unsigned
	// BlockSize defines size of data block Postgres operates.
	BlockSize int
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
	// PgStatStatementsSource defines the database name where pg_stat_statements is available
	PgStatStatementsSource string
}

// NewPostgresServiceConfig defines new config for Postgres-based collectors
func NewPostgresServiceConfig(connStr string) (PostgresServiceConfig, error) {
	var config = PostgresServiceConfig{}

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

	// Get Postgres block size.
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'block_size'").Scan(&setting)
	if err != nil {
		return config, err
	}
	bsize, err := strconv.Atoi(setting)
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
		log.Warnf("Postgres version is too old, some collectors functions won't work. Minimum required version is %s.", PostgresVMinStr)
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

	// Get shared_preload_libraries (for inspecting enabled extensions).
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'shared_preload_libraries'").Scan(&setting)
	if err != nil {
		return config, err
	}
	if strings.Contains(setting, "pg_stat_statements") {
		// Enable PgStatStatements, but leave empty PgStatStatementsSource, it will be filled at first execution of collector's Update method.
		config.PgStatStatements = true
		config.PgStatStatementsSource = ""
	} else {
		log.Info("pg_stat_statements is not found in shared_preload_libraries, disable pg_stat_statements metrics collection")
		config.PgStatStatements = false
	}

	return config, nil
}
