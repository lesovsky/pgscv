package pgscv

import (
	"encoding/json"
	"fmt"
	"github.com/barcodepro/pgscv/internal/log"
	"github.com/barcodepro/pgscv/internal/runtime"
	"github.com/barcodepro/pgscv/internal/service"
	"github.com/jackc/pgx/v4"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	defaultListenAddress     = "127.0.0.1:10090"
	defaultHost              = "127.0.0.1"
	defaultPgbouncerPort     = 6432
	defaultPostgresUsername  = "pgscv"
	defaultPostgresDbname    = "postgres"
	defaultPgbouncerUsername = "pgscv"
	defaultPgbouncerDbname   = "pgbouncer"

	defaultMetricsSendInterval = 60 * time.Second
)

type Config struct {
	BinaryVersion        string                       // version of the program, required for auto-update procedure
	RuntimeMode          int                          // application runtime mode
	ScheduleEnabled      bool                         // use schedule-based metrics collecting
	ListenAddress        string                       `json:"listen_address"`      // Network address and port where the application should listen on
	MetricsServiceURL    string                       `json:"metrics_service_url"` // URL of Weaponry service metric gateway
	MetricsSendInterval  time.Duration                // Metric send interval
	APIKey               string                       `json:"api_key"` // API key for accessing to Weaponry
	ProjectID            string                       // ProjectID value obtained from API key
	ServicesConnSettings []service.ServiceConnSetting `json:"services"` // Slice of connection settings for exact services
	Defaults             map[string]string            `json:"defaults"` // Defaults
}

func NewConfig(configFilePath string) (*Config, error) {
	content, err := ioutil.ReadFile(configFilePath)
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
	if c.APIKey != "" {
		projectID, err := newProjectID(c.APIKey)
		if err != nil {
			return err
		}
		c.ProjectID = projectID
	}

	if c.MetricsServiceURL == "" {
		c.RuntimeMode = runtime.PullMode
	} else {
		c.RuntimeMode = runtime.PushMode
		c.MetricsSendInterval = defaultMetricsSendInterval
		c.ScheduleEnabled = true
	}

	// API key is necessary when Metric Service is specified
	if c.MetricsServiceURL != "" && c.APIKey == "" {
		return fmt.Errorf("API key should be specified")
	}

	if c.ListenAddress == "" {
		c.ListenAddress = defaultListenAddress
	}

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
		var foundInvalid bool
		if len(c.ServicesConnSettings) != 0 {
			for i, s := range c.ServicesConnSettings {
				if s.ServiceType == "" {
					log.Errorf("service_type is not specified for %s; ignore", s.Conninfo)
					c.ServicesConnSettings[i].Conninfo = "__invalid__"
					foundInvalid = true
				}

				_, err := pgx.ParseConfig(s.Conninfo)
				if err != nil {
					log.Errorf("%s: %s; ignore", err, s.Conninfo)
					c.ServicesConnSettings[i].Conninfo = "__invalid__"
					foundInvalid = true
				}
			}
		}

		// If services with invalid Conninfo have been found, just build a new slice without invalid services.
		if foundInvalid {
			cc := make([]service.ServiceConnSetting, 0, len(c.ServicesConnSettings))
			for _, s := range c.ServicesConnSettings {
				if s.Conninfo != "__invalid__" {
					cc = append(cc, s)
				}
			}

			// But it may happen that all the passed settings are wrong, in this case don't allocate slice.
			if len(cc) != 0 {
				c.ServicesConnSettings = cc
			} else {
				c.ServicesConnSettings = nil
			}
		}
	}

	return nil
}

// newProjectID reads provided API key and produces ProjectID string
func newProjectID(s string) (string, error) {
	// sanity check, but normally should not be here
	if s == "" {
		return "", fmt.Errorf("api key not found")
	}

	log.Debug("processing api key")

	// api key should consists of four parts
	parts := strings.Split(s, "-")
	if len(parts) != 4 {
		return "", fmt.Errorf("api key bad format")
	}

	// lengths of these parts should be 12-4-4-8 (yes, this is not UUID)
	for i, v := range []int{12, 4, 4, 8} {
		if len(parts[i]) != v {
			return "", fmt.Errorf("api key bad format")
		}
	}

	re, err := regexp.Compile("^[A-Z0-9]+$")
	if err != nil {
		return "", err
	}

	// each part should satisfy the regexp
	for _, v := range parts {
		if !re.MatchString(v) {
			return "", fmt.Errorf("api key bad format")
		}
	}

	re, err = regexp.Compile("[A-Z]+")
	if err != nil {
		return "", err
	}

	// extract project_id from last part
	id := re.ReplaceAllString(parts[3], "")

	// check extracted value is able to be converted to int64
	_, err = strconv.ParseInt(id, 10, 64)
	if err != nil {
		return "", err
	}

	return id, nil
}
