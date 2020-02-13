//
package main

import (
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"os/user"
	"pgscv/app"
	"strings"
	"time"
)

var (
	binName, appName, gitCommit, gitBranch string
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})
	//log.Logger = log.With().Caller().Logger().Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})

	var (
		username             = serviceUsername()
		metricServiceBaseURL = kingpin.Flag("metric-service-url", "Metric service URL push to").Default("").Envar("METRIC_SERVICE_BASE_URL").String()
		metricsSendInterval  = kingpin.Flag("send-interval", "Interval between pushes").Default("60s").Envar("SEND_INTERVAL").Duration()
		projectIdStr         = kingpin.Flag("projectid", "Project identifier string").Envar("PROJECTID").String()
		bootstrapKey         = kingpin.Flag("bootstrap-key", "Run bootstrap using specified key, requires root privileges").Envar("BOOTSTRAP_KEY").String()
		postgresUsername     = kingpin.Flag("pg-username", "Username used for connecting to Postgres services").Default(username).Envar("PG_USERNAME").String()
		postgresPassword     = kingpin.Flag("pg-password", "Password used for connecting to Postgres services").Default("").Envar("PG_PASSWORD").String()
		pgbouncerUsername    = kingpin.Flag("pgb-username", "Username used for connecting to Pgbouncer services").Default(username).Envar("PGB_USERNAME").String()
		pgbouncerPassword    = kingpin.Flag("pgb-password", "Password used for connecting to Pgbouncer services").Default("").Envar("PGB_PASSWORD").String()
		showver              = kingpin.Flag("version", "show version and exit").Default().Bool()
		logLevel             = kingpin.Flag("log-level", "set log level: debug, info, warn, error").Default("info").Envar("LOG_LEVEL").String()
	)
	kingpin.Parse()

	var sc = &app.Config{
		Logger:               log.Logger,
		MetricServiceBaseURL: *metricServiceBaseURL,
		MetricsSendInterval:  *metricsSendInterval,
		ProjectIdStr:         *projectIdStr,
		ScheduleEnabled:      false,
		BootstrapKey:         *bootstrapKey,
		BootstrapBinaryName:  binName,
		Credentials: app.Credentials{
			PostgresUser:  *postgresUsername,
			PostgresPass:  *postgresPassword,
			PgbouncerUser: *pgbouncerUsername,
			PgbouncerPass: *pgbouncerPassword,
		},
	}

	switch *logLevel {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	if *showver {
		fmt.Printf("%s %s-%s\n", appName, gitCommit, gitBranch)
		os.Exit(0)
	}

	if sc.BootstrapKey != "" {
		os.Exit(app.RunBootstrap(sc))
	}

	// обязательно должен быть
	if sc.ProjectIdStr == "" {
		log.Fatal().Msg("project identifier is not specified")
	}

	// use schedulers in push mode
	if sc.MetricServiceBaseURL != "" {
		sc.ScheduleEnabled = true
	}

	if err := app.Start(sc); err != nil {
		log.Error().Err(err)
	}

	log.Info().Msg("Graceful shutdown")
}

// serviceUsername provides default username used for connecting to services, if no usernames provided by EnvVars.
func serviceUsername() string {
	var username = strings.ToLower(strings.Replace(binName, "-", "_", -1))
	u, err := user.Current()
	if err != nil {
		log.Warn().Err(err).Msgf("failed getting current username, use fallback username: %s", username)
		return username
	}
	return u.Username
}
