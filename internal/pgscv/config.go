package pgscv

import (
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/weaponry/pgscv/internal/filter"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/service"
	"gopkg.in/yaml.v2"
	"os"
	"path/filepath"
	"time"
)

const (
	defaultListenAddress     = "127.0.0.1:9890"
	defaultPostgresUsername  = "pgscv"
	defaultPostgresDbname    = "postgres"
	defaultPgbouncerUsername = "pgscv"
	defaultPgbouncerDbname   = "pgbouncer"

	defaultSendMetricsInterval = 60 * time.Second
)

// Config defines application's configuration.
type Config struct {
	BinaryPath            string                // full path of the program, required for auto-update procedure
	BinaryVersion         string                // version of the program, required for auto-update procedure
	AutoUpdate            string                `yaml:"autoupdate"`       // controls auto-update procedure
	NoTrackMode           bool                  `yaml:"no_track_mode"`    // controls tracking sensitive information (query texts, etc)
	ListenAddress         string                `yaml:"listen_address"`   // Network address and port where the application should listen on
	SendMetricsURL        string                `yaml:"send_metrics_url"` // URL of Weaponry service metric gateway
	SendMetricsInterval   time.Duration         // Metric send interval
	APIKey                string                `yaml:"api_key"`  // API key for accessing to Weaponry
	ServicesConnsSettings service.ConnsSettings `yaml:"services"` // All connections settings for exact services
	Defaults              map[string]string     `yaml:"defaults"` // Defaults
	Filters               filter.Filters        `yaml:"filters"`
	DisableCollectors     []string              `yaml:"disable_collectors"` // List of collectors which should be disabled. DEPRECATED in favor collectors settings
	Collectors            model.Collectors      `yaml:"collectors"`         // Collectors and its settings
}

// NewConfig creates new config based on config file or return default config of config is not exists.
func NewConfig(configFilePath string) (*Config, error) {
	if configFilePath == "" {
		return &Config{Defaults: map[string]string{}}, nil
	}

	content, err := os.ReadFile(filepath.Clean(configFilePath))
	if err != nil {
		return nil, err
	}

	config := Config{Defaults: map[string]string{}}
	err = yaml.Unmarshal(content, &config)
	if err != nil {
		return nil, err
	}

	log.Infoln("read configuration from ", configFilePath)
	return &config, nil
}

// Validate checks configuration for stupid values and set defaults
func (c *Config) Validate() error {
	c.SendMetricsInterval = defaultSendMetricsInterval

	// API key is necessary when Metric Service is specified
	if c.SendMetricsURL != "" && c.APIKey == "" {
		return fmt.Errorf("API key should be specified")
	}

	if c.ListenAddress == "" {
		c.ListenAddress = defaultListenAddress
	}

	log.Infoln("*** IMPORTANT ***: pgSCV by default collects information about user queries. Tracking queries can be disabled with 'no_track_mode: true' in config file.")
	if c.NoTrackMode {
		log.Infoln("no-track mode enabled: tracking disabled for [pg_stat_statements.query].")
	} else {
		log.Infoln("no-track mode disabled")
	}

	// Process auto-update setting.
	v, err := toggleAutoupdate(c.AutoUpdate)
	if err != nil {
		return err
	}

	c.AutoUpdate = v

	// setup defaults
	if c.Defaults == nil {
		c.Defaults = map[string]string{}
	}

	if _, ok := c.Defaults["postgres_username"]; !ok {
		c.Defaults["postgres_username"] = defaultPostgresUsername
	}

	if _, ok := c.Defaults["postgres_dbname"]; !ok {
		c.Defaults["postgres_dbname"] = defaultPostgresDbname
	}

	if _, ok := c.Defaults["pgbouncer_username"]; !ok {
		c.Defaults["pgbouncer_username"] = defaultPgbouncerUsername
	}

	if _, ok := c.Defaults["pgbouncer_dbname"]; !ok {
		c.Defaults["pgbouncer_dbname"] = defaultPgbouncerDbname
	}

	// User might specify its own set of services which he would like to monitor. This services should be validated and
	// invalid should be rejected. Validation is performed using pgx.ParseConfig method which does all dirty work.
	if c.ServicesConnsSettings != nil {
		if len(c.ServicesConnsSettings) != 0 {
			for k, s := range c.ServicesConnsSettings {
				if k == "" {
					return fmt.Errorf("empty service specified")
				}
				if s.ServiceType == "" {
					return fmt.Errorf("empty service_type for %s", k)
				}

				_, err := pgx.ParseConfig(s.Conninfo)
				if err != nil {
					return fmt.Errorf("invalid conninfo for %s: %s", k, err)
				}
			}
		}
	}

	// Add default filters and compile regexps.
	if c.Filters == nil {
		c.Filters = filter.New()
	}
	c.Filters.SetDefault()
	if err := c.Filters.Compile(); err != nil {
		return err
	}

	return nil
}

// toggleAutoupdate control auto-update setting.
func toggleAutoupdate(value string) (string, error) {
	// Empty value explicitly set to 'off'.
	if value == "" {
		return "off", nil
	}

	// Valid values are 'devel', 'stable' and 'off'. All other are invalid.
	switch value {
	case "devel", "stable", "off":
		return value, nil
	default:
		return "", fmt.Errorf("invalid value '%s' for 'autoupdate'", value)
	}
}
