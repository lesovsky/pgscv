package model

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"github.com/shirou/gopsutil/process"
)

const (
	// ServiceTypeDisabled ...
	ServiceTypeDisabled = iota
	// ServiceTypePostgresql ...
	ServiceTypePostgresql
	// ServiceTypePgbouncer ...
	ServiceTypePgbouncer
	// ServiceTypeSystem ...
	ServiceTypeSystem

	// DefaultServiceUser specifies default username for connecting to services
	DefaultServiceUser = "weaponry_app"

	// DefaultServiceHost specifies default hostname for connecting to services
	DefaultServiceHost = "127.0.0.1"

	// DefaultPostgresPort specifies default port number for connecting to Postgres services
	DefaultPostgresPort = 5432

	// DefaultPostgresDbname specifies default database name when connecting to Postgres services
	DefaultPostgresDbname = "postgres"

	// DefaultPgbouncerPort specifies default port number for connecting to Pgbouncer services
	DefaultPgbouncerPort = 6432

	// DefaultPgbouncerDbname specifies default database name when connecting to Pgbouncer services
	DefaultPgbouncerDbname = "pgbouncer"
)

// Service describes discovered service
type Service struct {
	ServiceType       int      // Type specifies which kind of metrics should be associated with instance (postgresql, pgbouncer, etc...)
	ServiceID         string   // Service identifier is unique key across all instances
	ProjectID         string   // Project identifier is unique key across all projects (project may have several instances)
	Pid               int32    // Process identifier
	ProcessName       string   // Process name
	ProcessCreateTime int64    // Process create time in milliseconds since the epoch, in UTC
	Host              string   // Hostname used as part of a connection string (related to postgresql, pgbouncer)
	Port              uint16   // Port used as part of a connection string (related to postgresql, pgbouncer)
	User              string   // Username used as part of a connection string (related to postgresql, pgbouncer)
	Password          string   // Password used as part of a connection string (related to postgresql, pgbouncer)
	Dbname            string   // Database name used as part of a connection string (related to postgresql, pgbouncer)
	Exporter          Exporter // Exporter associated with instance
}

// Validate checks service settings and adjust if required
func (s *Service) Validate() {
	if s.Host == "" {
		s.Host = DefaultServiceHost
	}
	if s.User == "" {
		s.User = DefaultServiceUser
	}

	switch s.ServiceType {
	case ServiceTypePostgresql:
		if s.Port == 0 {
			s.Port = DefaultPostgresPort
		}
		if s.Dbname == "" {
			s.Dbname = DefaultPostgresDbname
		}
	case ServiceTypePgbouncer:
		if s.Port == 0 {
			s.Port = DefaultPgbouncerPort
		}
		if s.Dbname == "" {
			s.Dbname = DefaultPgbouncerDbname
		}
	}
}

// IsAvailable checks the process associated with the service is still exists in processes list and has the same name and created time
func (s *Service) IsAvailable() bool {
	// system service always available, skip it
	if s.ServiceType == ServiceTypeSystem {
		return true
	}

	// check process with such pid still exists, NewProcess internally checks process existence
	proc, err := process.NewProcess(s.Pid)
	if err != nil {
		log.Warn().Err(err).Msg("service disappeared")
		return false
	}

	// check process's name
	name, err := proc.Name()
	if err != nil {
		log.Warn().Err(err).Msg("failed get process name")
		return false
	}
	if s.ProcessName != name {
		log.Warn().Msgf("process exists, but has different name (%s)", name)
		return false
	}

	// check process's create time
	ctime, err := proc.CreateTime()
	if err != nil {
		log.Warn().Err(err).Msg("get process create time failed")
		return false // mark the service as unavailable even if getting the create time is failed
	}

	if s.ProcessCreateTime != ctime {
		log.Warn().Msgf("process exists, but has different create time")
		return false
	}

	// OK, process with the same pid, name and create time exists
	return true
}

// Exporter is an interface for prometheus.Collector
type Exporter interface {
	Describe(chan<- *prometheus.Desc)
	Collect(chan<- prometheus.Metric)
}
