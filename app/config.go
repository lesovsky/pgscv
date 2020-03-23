package app

import (
	"github.com/rs/zerolog"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Config struct describes the application's configuration
type Config struct {
	Logger               zerolog.Logger
	ProjectIDStr         string
	MetricServiceBaseURL string
	MetricsSendInterval  time.Duration
	ScheduleEnabled      bool
	DiscoveryEnabled     bool
	APIKey               string
	BootstrapBinaryName  string
	URLStrings           []string
	Credentials          Credentials
}

// Credentials struct describes default requisites defined by user and used for connecting to services
type Credentials struct {
	PostgresUser  string
	PostgresPass  string
	PgbouncerUser string
	PgbouncerPass string
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
