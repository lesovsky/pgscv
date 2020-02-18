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
	ProjectIdStr         string
	MetricServiceBaseURL string
	MetricsSendInterval  time.Duration
	ScheduleEnabled      bool
	ApiKey               string
	BootstrapBinaryName  string
	Credentials          Credentials
}

// Credentials struct describes requisites defined by user and used for connecting to services
type Credentials struct {
	PostgresUser  string
	PostgresPass  string
	PgbouncerUser string
	PgbouncerPass string
}

func DecodeProjectIdStr(s string) string {
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
