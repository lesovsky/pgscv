package model

import "github.com/prometheus/client_golang/prometheus"

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
	ServiceType int      // Type specifies which kind of metrics should be associated with instance (postgresql, pgbouncer, etc...)
	ServiceID   string   // Service identifier is unique key across all instances
	ProjectID   string   // Project identifier is unique key across all projects (project may have several instances)
	Pid         int32    // Process identifier
	Host        string   // Hostname used as part of a connection string (related to postgresql, pgbouncer)
	Port        uint16   // Port used as part of a connection string (related to postgresql, pgbouncer)
	User        string   // Username used as part of a connection string (related to postgresql, pgbouncer)
	Password    string   // Password used as part of a connection string (related to postgresql, pgbouncer)
	Dbname      string   // Database name used as part of a connection string (related to postgresql, pgbouncer)
	Exporter    Exporter // Exporter associated with instance
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

// Exporter is an interface for prometheus.Collector
type Exporter interface {
	Describe(chan<- *prometheus.Desc)
	Collect(chan<- prometheus.Metric)
}
