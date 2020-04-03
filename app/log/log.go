package log

import (
	"fmt"
	"github.com/rs/zerolog"
	"os"
	"time"
)

// Logger is the global logger with predefined settings
var Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).With().Timestamp().Logger()

// SetLevel sets logging level
func SetLevel(level string) {
	switch level {
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
}

// ExtendWithStr extends logger with extra key=value pair (all previously added pairs are not saved)
func ExtendWithStr(name, value string) {
	Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).With().Timestamp().Str(name, value).Logger()
}

// Debug prints message with DEBUG severity
func Debug(msg string) {
	Logger.Debug().Msg(msg)
}

// Debugf prints formatted message with DEBUG severity
func Debugf(format string, v ...interface{}) {
	Logger.Debug().Msgf(format, v...)
}

// Debugln concatenates arguments and prints them with DEBUG severity
func Debugln(v ...interface{}) {
	Logger.Debug().Msg(fmt.Sprint(v...))
}

// Info prints message with INFO severity
func Info(msg string) {
	Logger.Info().Msg(msg)
}

// Infof prints formatted message with INFO severity
func Infof(format string, v ...interface{}) {
	Logger.Info().Msgf(format, v...)
}

// Infoln concatenates arguments and prints them with INFO severity
func Infoln(v ...interface{}) {
	Logger.Info().Msg(fmt.Sprint(v...))
}

// Warn prints message with WARNING severity
func Warn(msg string) {
	Logger.Warn().Msg(msg)
}

// Warnf prints formatted message with WARNING severity
func Warnf(format string, v ...interface{}) {
	Logger.Warn().Msgf(format, v...)
}

// Warnln concatenates arguments and prints them with WARNING severity
func Warnln(v ...interface{}) {
	Logger.Warn().Msg(fmt.Sprint(v...))
}

// Error prints message with ERROR severity
func Error(msg string) {
	Logger.Error().Msg(msg)
}

// Errorf prints formatted message with ERROR severity
func Errorf(format string, v ...interface{}) {
	Logger.Error().Msgf(format, v...)
}

// Errorln concatenates arguments and prints them with ERROR severity
func Errorln(v ...interface{}) {
	Logger.Error().Msg(fmt.Sprint(v...))
}
