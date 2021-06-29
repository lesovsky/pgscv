package pgscv

import (
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/service"
	"gopkg.in/yaml.v2"
	"os"
	"path/filepath"
	"regexp"
	"strings"
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
	BinaryPath            string                   // full path of the program, required for auto-update procedure
	BinaryVersion         string                   // version of the program, required for auto-update procedure
	AutoUpdate            string                   `yaml:"autoupdate"`       // controls auto-update procedure
	NoTrackMode           bool                     `yaml:"no_track_mode"`    // controls tracking sensitive information (query texts, etc)
	ListenAddress         string                   `yaml:"listen_address"`   // Network address and port where the application should listen on
	SendMetricsURL        string                   `yaml:"send_metrics_url"` // URL of Weaponry service metric gateway
	SendMetricsInterval   time.Duration            // Metric send interval
	APIKey                string                   `yaml:"api_key"`            // API key for accessing to Weaponry
	ServicesConnsSettings service.ConnsSettings    `yaml:"services"`           // All connections settings for exact services
	Defaults              map[string]string        `yaml:"defaults"`           // Defaults
	DisableCollectors     []string                 `yaml:"disable_collectors"` // List of collectors which should be disabled. DEPRECATED in favor collectors settings
	CollectorsSettings    model.CollectorsSettings `yaml:"collectors"`         // Collectors settings propagated from main YAML configuration
	Databases             string                   `yaml:"databases"`          // Regular expression string specifies databases from which metrics should be collected
	DatabasesRE           *regexp.Regexp           // Regular expression object compiled from Databases
}

// NewConfig creates new config based on config file or return default config if config file is not specified.
func NewConfig(configFilePath string) (*Config, error) {
	if configFilePath == "" {
		return newConfigFromEnv()
	}

	log.Infoln("read configuration from ", configFilePath)
	content, err := os.ReadFile(filepath.Clean(configFilePath))
	if err != nil {
		return nil, err
	}

	config := &Config{Defaults: map[string]string{}}

	err = yaml.Unmarshal(content, config)
	if err != nil {
		return nil, err
	}

	return config, nil
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

	if c.NoTrackMode {
		log.Infoln("no-track enabled for [pg_stat_statements.query].")
	} else {
		log.Infoln("no-track disabled, for details check the documentation about 'no_track_mode' option.")
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

	// Create 'databases' regexp object for builtin metrics.
	re, err := newDatabasesRegexp(c.Databases)
	if err != nil {
		return err
	}
	c.DatabasesRE = re

	// Validate collector settings.
	err = validateCollectorSettings(c.CollectorsSettings)
	if err != nil {
		return err
	}

	return nil
}

// validateCollectorSettings validates collectors settings passed from main YAML configuration.
func validateCollectorSettings(cs model.CollectorsSettings) error {
	if cs == nil || len(cs) == 0 {
		return nil
	}

	for csName, settings := range cs {
		re1 := regexp.MustCompile(`^[a-zA-Z0-9]+/[a-zA-Z0-9]+$`)
		if !re1.MatchString(csName) {
			return fmt.Errorf("invalid collector name: %s", csName)
		}

		err := settings.Filters.Compile()
		if err != nil {
			return err
		}

		// Validate subsystems level
		for ssName, subsys := range settings.Subsystems {
			re2 := regexp.MustCompilePOSIX(`^[a-zA-Z0-9_]+$`)

			if !re2.MatchString(ssName) {
				return fmt.Errorf("invalid subsystem name: %s", ssName)
			}

			// Validate databases regexp.
			_, err := regexp.Compile(subsys.Databases)
			if err != nil {
				return fmt.Errorf("databases invalid regular expression specified: %s", err)
			}

			// Query must be specified if any metrics.
			if len(subsys.Metrics) > 0 && subsys.Query == "" {
				return fmt.Errorf("query is not specified for subsystem '%s' metrics", ssName)
			}

			// Validate metrics level
			reMetric := regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

			for _, m := range subsys.Metrics {
				if m.Value == "" && m.LabeledValues == nil {
					return fmt.Errorf("value or labeled_values should be specified for metric '%s'", m.ShortName)
				}

				if m.Value != "" && m.LabeledValues != nil {
					return fmt.Errorf("value and labeled_values cannot be used together for metric '%s'", m.ShortName)
				}

				usage := m.Usage
				switch usage {
				case "COUNTER", "GAUGE":
					if !reMetric.MatchString(m.ShortName) {
						return fmt.Errorf("invalid metric name '%s'", m.ShortName)
					}
					if m.Description == "" {
						return fmt.Errorf("metric description is not specified for %s", m.ShortName)
					}
				default:
					return fmt.Errorf("invalid metric usage '%s'", usage)
				}
			}
		}
	}

	return nil
}

// newConfigFromEnv create config using environment variables.
func newConfigFromEnv() (*Config, error) {
	log.Infoln("read configuration from environment")

	config := &Config{
		Defaults: map[string]string{},
	}

	for _, env := range os.Environ() {
		if !strings.HasPrefix(env, "PGSCV_") &&
			!strings.HasPrefix(env, "POSTGRES_DSN") &&
			!strings.HasPrefix(env, "DATABASE_DSN") &&
			!strings.HasPrefix(env, "PGBOUNCER_DSN") &&
			!strings.HasPrefix(env, "PATRONI_URL") {
			continue
		}

		if config.ServicesConnsSettings == nil {
			config.ServicesConnsSettings = map[string]service.ConnSetting{}
		}

		ff := strings.SplitN(env, "=", 2)

		key, value := ff[0], ff[1]

		// Parse POSTGRES_DSN (or its alias DATABASE_DSN).
		if strings.HasPrefix(key, "POSTGRES_DSN") || strings.HasPrefix(key, "DATABASE_DSN") {
			id, cs, err := service.ParsePostgresDSNEnv(key, value)
			if err != nil {
				return nil, err
			}

			config.ServicesConnsSettings[id] = cs
		}

		// Parse PGBOUNCER_DSN.
		if strings.HasPrefix(key, "PGBOUNCER_DSN") {
			id, cs, err := service.ParsePgbouncerDSNEnv(key, value)
			if err != nil {
				return nil, err
			}

			config.ServicesConnsSettings[id] = cs
		}

		// Parse PATRONI_URL.
		if strings.HasPrefix(key, "PATRONI_URL") {
			id, cs, err := service.ParsePatroniURLEnv(key, value)
			if err != nil {
				return nil, err
			}

			config.ServicesConnsSettings[id] = cs
		}

		switch key {
		case "PGSCV_LISTEN_ADDRESS":
			config.ListenAddress = value
		case "PGSCV_AUTOUPDATE":
			config.AutoUpdate = value
		case "PGSCV_NO_TRACK_MODE":
			switch value {
			case "y", "yes", "Yes", "YES", "t", "true", "True", "TRUE", "1", "on":
				config.NoTrackMode = true
			default:
				config.NoTrackMode = false
			}
		case "PGSCV_SEND_METRICS_URL":
			config.SendMetricsURL = value
		case "PGSCV_API_KEY":
			config.APIKey = value
		case "PGSCV_DATABASES":
			config.Databases = value
		case "PGSCV_DISABLE_COLLECTORS":
			config.DisableCollectors = strings.Split(strings.Replace(value, " ", "", -1), ",")
		}
	}

	return config, nil
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

// newDatabasesRegexp creates new regexp depending on passed string.
func newDatabasesRegexp(s string) (*regexp.Regexp, error) {
	if s == "" {
		s = ".+"
	}

	return regexp.Compile(s)
}
