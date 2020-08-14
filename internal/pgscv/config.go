package pgscv

import (
	"encoding/json"
	"fmt"
	"github.com/barcodepro/pgscv/internal/filter"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/model"
	"github.com/barcodepro/pgscv/internal/service"
	"github.com/jackc/pgx/v4"
	"io/ioutil"
	"path/filepath"
	"time"
)

const (
	defaultListenAddress     = "127.0.0.1:10090"
	defaultPostgresUsername  = "pgscv"
	defaultPostgresDbname    = "postgres"
	defaultPgbouncerUsername = "pgscv"
	defaultPgbouncerDbname   = "pgbouncer"

	defaultMetricsSendInterval = 60 * time.Second
)

// Config defines application's configuration.
type Config struct {
	BinaryVersion        string                   // version of the program, required for auto-update procedure
	AutoUpdateURL        string                   `json:"autoupdate_url"` // URL used for auto-update
	RuntimeMode          int                      // application runtime mode
	AllowTrackSensitive  bool                     `json:"allow_track_sensitive"` // controls tracking sensitive information (query texts, etc)
	ListenAddress        string                   `json:"listen_address"`        // Network address and port where the application should listen on
	MetricsServiceURL    string                   `json:"metrics_service_url"`   // URL of Weaponry service metric gateway
	MetricsSendInterval  time.Duration            // Metric send interval
	APIKey               string                   `json:"api_key"`    // API key for accessing to Weaponry
	ProjectID            int                      `json:"project_id"` // ProjectID specifies project_id label value
	ServicesConnSettings []service.ConnSetting    `json:"services"`   // Slice of connection settings for exact services
	Defaults             map[string]string        `json:"defaults"`   // Defaults
	Filters              map[string]filter.Filter `json:"filters"`
}

// NewConfig creates new config based on config file.
func NewConfig(configFilePath string) (*Config, error) {
	content, err := ioutil.ReadFile(filepath.Clean(configFilePath))
	if err != nil {
		return nil, err
	}

	config := Config{Defaults: map[string]string{}}
	err = json.Unmarshal(content, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

// Validate checks configuration for stupid values and set defaults
func (c *Config) Validate() error {
	if (c.APIKey != "" && c.ProjectID == 0) || (c.APIKey == "" && c.ProjectID != 0) {
		return fmt.Errorf("API key and Project ID should be specified both")
	}

	if c.MetricsServiceURL == "" {
		c.RuntimeMode = model.RuntimePullMode
	} else {
		c.RuntimeMode = model.RuntimePushMode
		c.MetricsSendInterval = defaultMetricsSendInterval
	}

	// API key is necessary when Metric Service is specified
	if c.MetricsServiceURL != "" && c.APIKey == "" {
		return fmt.Errorf("API key should be specified")
	}

	if c.ListenAddress == "" {
		c.ListenAddress = defaultListenAddress
	}

	if c.AllowTrackSensitive {
		log.Infoln("sensitive data tracking: enabled. Enabled for: pg_stat_statements.query.")
	} else {
		log.Infoln("sensitive data tracking: disabled.")
	}
	log.Infoln("HINT: use 'allow_track_sensitive: <bool>' config option to manage.")

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
	if c.ServicesConnSettings != nil {
		if len(c.ServicesConnSettings) != 0 {
			for _, s := range c.ServicesConnSettings {
				if s.ServiceType == "" {
					return fmt.Errorf("service_type is not specified for %s", s.Conninfo)
				}

				_, err := pgx.ParseConfig(s.Conninfo)
				if err != nil {
					return fmt.Errorf("invalid conninfo: %s", err)
				}
			}
		}
	}

	// Add default filters and compile regexps.
	if c.Filters == nil {
		c.Filters = make(map[string]filter.Filter)
	}
	filter.DefaultFilters(c.Filters)
	if err := filter.CompileFilters(c.Filters); err != nil {
		return err
	}

	return nil
}
