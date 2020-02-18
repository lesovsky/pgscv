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
		doBootstrap          = kingpin.Flag("bootstrap", "Run bootstrap, requires root privileges").Default("false").Envar("BOOTSTRAP").Bool()
		apiKey               = kingpin.Flag("api-key", "Use api key").Default("").Envar("API_KEY").String()
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
		ProjectIdStr:         app.DecodeProjectIdStr(*apiKey),
		ScheduleEnabled:      false,
		ApiKey:               *apiKey,
		BootstrapBinaryName:  binName,
		Credentials: app.Credentials{
			PostgresUser:  *postgresUsername,
			PostgresPass:  *postgresPassword,
			PgbouncerUser: *pgbouncerUsername,
			PgbouncerPass: *pgbouncerPassword,
		},
	}

	// TODO: add config validations, for: 1) api-key 2) send-interval 3) etc...

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

	if *doBootstrap {
		os.Exit(app.RunBootstrap(sc))
	}

	// если указан апи-ключ, то из него по-любому должен быть вытащен ид проекта
	if sc.ApiKey != "" && sc.ProjectIdStr == "" {
		log.Fatal().Msg("unknown project identifier")
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
	var usernameFallback = strings.ToLower(strings.Replace(binName, "-", "_", -1))
	u, err := user.Current()
	if err != nil {
		log.Warn().Err(err).Msgf("failed getting current username, use fallback username: %s", usernameFallback)
		return usernameFallback
	}
	return u.Username
}
