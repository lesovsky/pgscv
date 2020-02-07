package app

import (
	"github.com/rs/zerolog"
	"time"
)

type Config struct {
	Logger               zerolog.Logger
	ProjectIdStr         string
	MetricServiceBaseURL string
	MetricsSendInterval  time.Duration
	ScheduleEnabled      bool
	BootstrapKey         string
}
