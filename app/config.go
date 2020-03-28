package app

import (
	"fmt"
	"github.com/rs/zerolog"
	"net"
	"net/url"
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
	Logger               zerolog.Logger
	RuntimeMode          int
	ProjectIDStr         string
	ListenAddress        net.TCPAddr
	MetricServiceBaseURL url.URL
	MetricsSendInterval  time.Duration
	ScheduleEnabled      bool
	APIKey               string
	BootstrapBinaryName  string
	DefaultCredentials
}

type DefaultCredentials struct {
	PostgresPassword  string
	PgbouncerPassword string
}

func (c *Config) Validate() error {
	if c.MetricServiceBaseURL.String() == "" {
		c.RuntimeMode = runtimeModePull
	} else {
		c.RuntimeMode = runtimeModePush
		c.ScheduleEnabled = true
	}

	if c.APIKey != "" && c.ProjectIDStr == "" {
		return fmt.Errorf("project identifier is not specified")
	}

	return nil
}

// DecodeProjectIDStr ...
func DecodeProjectIDStr(s string) string {
	reAlpha, err := regexp.Compile("[A-Z]+")
	if err != nil {
		return ""
	}
	// split api key
	parts := strings.Split(s, "-")
	if len(parts) != 4 {
		return ""
	}

	// extract project_id from last part
	id := reAlpha.ReplaceAllString(parts[3], "")

	// check extracted value is convertable to int64
	_, err = strconv.ParseInt(id, 10, 64)
	if err != nil {
		return ""
	}
	return id
}
