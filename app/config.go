package app

import (
	"fmt"
	"net"
	"net/url"
	"pgscv/app/log"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	runtimeModePull int = 1
	runtimeModePush int = 2
)

// Config struct describes the application's configuration
type Config struct {
	RuntimeMode          int
	ProjectIDStr         string
	ListenAddress        net.TCPAddr
	MetricServiceBaseURL url.URL
	MetricsSendInterval  time.Duration
	ScheduleEnabled      bool
	APIKey               string
	BinaryName           string
	BinaryVersion        string
	DefaultCredentials
}

// DefaultCredentials presents default passwords used for connecting to services
type DefaultCredentials struct {
	PostgresPassword  string
	PgbouncerPassword string
}

// Validate performs Config validations
func (c *Config) Validate() error {
	if c.MetricServiceBaseURL.String() == "" {
		c.RuntimeMode = runtimeModePull
	} else {
		c.RuntimeMode = runtimeModePush
		c.ScheduleEnabled = true
	}

	// should fail in case when API key is provided by user, but agent can't extract ProjectID from it
	if c.APIKey != "" && c.ProjectIDStr == "" {
		return fmt.Errorf("unknown project id")
	}

	return nil
}

// DecodeProjectIDStr decodes ProjectID from provided API key
func DecodeProjectIDStr(s string) string {
	if s == "" {
		log.Info("no api key provided")
		return ""
	}

	log.Debug("processing api key")

	re, err := regexp.Compile("[A-Z]+")
	if err != nil {
		log.Errorln("processing api key failed: ", err)
		return ""
	}
	// split api key
	parts := strings.Split(s, "-")
	if len(parts) != 4 {
		log.Error("processing api key failed: bad format")
		return ""
	}

	// extract project_id from last part
	id := re.ReplaceAllString(parts[3], "")

	// check extracted value is convertable to int64
	_, err = strconv.ParseInt(id, 10, 64)
	if err != nil {
		log.Errorln("processing api key failed: ", err)
		return ""
	}
	return id
}
