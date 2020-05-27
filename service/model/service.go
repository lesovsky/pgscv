package model

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	ServiceTypeDisabled   = "disabled"
	ServiceTypeSystem     = "system"
	ServiceTypePostgresql = "postgres"
	ServiceTypePgbouncer  = "pgbouncer"
)

// Service struct describes service - the target from which should be collected metrics.
type Service struct {
	// Service identifier is unique key across all monitored services and used to distinguish services of the same type
	// running on the single host (two Postgres services running on the same host but listening on different ports).
	// Hence not to mix their metrics the ServiceID is introduced and attached to metrics as "sid" label:
	// metric_xact_commits{database="test", sid="postgres:5432"} -- metric from the first postgres running on 5432 port
	// metric_xact_commits{database="test", sid="postgres:5433"} -- metric from the second postgres running on 5433 port
	ServiceID string
	// Project identifier is unique key across all projects (project may have several instances). ProjectID also is attached
	// as a label and unions metrics collected from the several hosts. See example below, there are two metrics from different
	// hosts but these hosts belong to the same "project" with ID = 1.
	// metric_xact_rollbacks{db_instance="host-1" sid="postgres:5432", database="test", project_id="1"}
	// metric_xact_rollbacks{db_instance="host-2" sid="postgres:5432", database="test", project_id="1"}
	ProjectID string
	// Connection settings required for connecting to the service.
	ConnSettings ServiceConnSetting
	// Prometheus-based metrics exporter associated with the service.
	Exporter Exporter
}

// ServiceConnSetting describes connection settings required for connecting to particular service. This struct primarily
// is used for representing services defined by user in the config file.
type ServiceConnSetting struct {
	ServiceType string `json:"service_type"`
	Conninfo    string `json:"conninfo"`
}

// Exporter is an interface for prometheus.Collector.
type Exporter interface {
	Describe(chan<- *prometheus.Desc)
	Collect(chan<- prometheus.Metric)
}
