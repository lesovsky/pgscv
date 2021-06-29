package service

import (
	"fmt"
	"github.com/weaponry/pgscv/internal/model"
	"strings"
)

// ConnSetting describes connection settings required for connecting to particular service.
// This is primarily used for describing services defined by user in the config file (or env vars).
type ConnSetting struct {
	// ServiceType defines type of service for which these connection settings are used.
	ServiceType string `yaml:"service_type"`
	// Conninfo is the connection string in service-specific format.
	Conninfo string `yaml:"conninfo"`
	// BaseURL is the base URL for connecting to HTTP services.
	BaseURL string `yaml:"baseurl"`
}

// ConnsSettings defines a set of all connection settings of exact services.
type ConnsSettings map[string]ConnSetting

// ParsePostgresDSNEnv is a public wrapper over parseDSNEnv.
func ParsePostgresDSNEnv(key, value string) (string, ConnSetting, error) {
	return parseDSNEnv("POSTGRES_DSN", strings.Replace(key, "DATABASE_DSN", "POSTGRES_DSN", 1), value)
}

// ParsePgbouncerDSNEnv is a public wrapper over parseDSNEnv.
func ParsePgbouncerDSNEnv(key, value string) (string, ConnSetting, error) {
	return parseDSNEnv("PGBOUNCER_DSN", key, value)
}

// ParsePatroniURLEnv is a public wrapper over parseURLEnv.
func ParsePatroniURLEnv(key, value string) (string, ConnSetting, error) {
	return parseURLEnv("PATRONI_URL", key, value)
}

// parseDSNEnv returns valid ConnSetting accordingly to passed prefix and environment key/value.
func parseDSNEnv(prefix, key, value string) (string, ConnSetting, error) {
	var stype string
	switch prefix {
	case "POSTGRES_DSN":
		stype = model.ServiceTypePostgresql
	case "PGBOUNCER_DSN":
		stype = model.ServiceTypePgbouncer
	default:
		return "", ConnSetting{}, fmt.Errorf("invalid prefix %s", prefix)
	}

	// Prefix must be the part of key.
	if !strings.HasPrefix(key, prefix) {
		return "", ConnSetting{}, fmt.Errorf("invalid key %s", key)
	}

	// Nothing to parse if prefix and key are the same, just use the type as service ID.
	if key == prefix {
		return stype, ConnSetting{ServiceType: stype, Conninfo: value}, nil
	}

	// If prefix and key are not the same, strip prefix from key and use the rest as service ID.
	// Use double Trim to avoid leaking 'prefix' string into ID value (see unit tests for examples).
	id := strings.TrimPrefix(strings.TrimPrefix(key, prefix), "_")

	if id == "" {
		return "", ConnSetting{}, fmt.Errorf("invalid value '%s' is in %s", value, key)
	}

	return id, ConnSetting{ServiceType: stype, Conninfo: value}, nil
}

// parseURLEnv returns valid ConnSetting accordingly to passed prefix and environment key/value.
func parseURLEnv(prefix, key, value string) (string, ConnSetting, error) {
	var stype string
	switch prefix {
	case "PATRONI_URL":
		stype = model.ServiceTypePatroni
	default:
		return "", ConnSetting{}, fmt.Errorf("invalid prefix %s", prefix)
	}

	// Prefix must be the part of key.
	if !strings.HasPrefix(key, prefix) {
		return "", ConnSetting{}, fmt.Errorf("invalid key %s", key)
	}

	// Nothing to parse if prefix and key are the same, just use the type as service ID.
	if key == prefix {
		return stype, ConnSetting{ServiceType: stype, BaseURL: value}, nil
	}

	// If prefix and key are not the same, strip prefix from key and use the rest as service ID.
	// Use double Trim to avoid leaking 'prefix' string into ID value (see unit tests for examples).
	id := strings.TrimPrefix(strings.TrimPrefix(key, prefix), "_")

	if id == "" {
		return "", ConnSetting{}, fmt.Errorf("invalid value '%s' is in %s", value, key)
	}

	return id, ConnSetting{ServiceType: stype, BaseURL: value}, nil
}
