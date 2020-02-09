package app

import (
	"github.com/rs/zerolog"
	"time"
)

// Config struct describes the application's configuration
type Config struct {
	Logger               zerolog.Logger
	ProjectIdStr         string
	MetricServiceBaseURL string
	MetricsSendInterval  time.Duration
	ScheduleEnabled      bool
	BootstrapKey         string
	Credentials          Credentials
}

// Credentials struct describes requisites defined by user and used for connecting to services
type Credentials struct {
	PostgresUser  string
	PostgresPass  string
	PgbouncerUser string
	PgbouncerPass string
}
