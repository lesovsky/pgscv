package app

import (
	"github.com/barcodepro/pgscv/service/internal/log"
	"github.com/barcodepro/pgscv/service/model"
	"github.com/prometheus/client_golang/prometheus"
	"os"
)

const (
	// default size of the catalog slice used for storing stats descriptors
	localCatalogDefaultSize = 10

	// how many failures should occur before unregistering exporter
	exporterFailureLimit int = 10
)

// prometheusExporter joins all necessary data for performing collecting metrics from services in the repo.
type prometheusExporter struct {
	ServiceID   string                      // unique ID across all services
	AllDesc     map[string]*prometheus.Desc // metrics assigned to this exporter
	ServiceRepo *ServiceRepo                // service repository
	statCatalog statCatalog                 // catalog of stat descriptors that belong only to that exporter
	totalFailed int                         // total number of collecting failures
}

// newExporter creates a new configured metrics exporter.
func newExporter(service model.Service, repo *ServiceRepo) (*prometheusExporter, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	var (
		itype     = service.ConnSettings.ServiceType
		projectid = service.ProjectID
		sid       = service.ServiceID
	)

	var e = make(map[string]*prometheus.Desc)
	var globalHelpCatalog = globalHelpCatalog()
	var globalCatalog = globalStatCatalog()
	var localCatalog = make([]statDescriptor, localCatalogDefaultSize)

	// walk through the stats descriptor catalog, select appropriate stats depending on service type and add the to local
	// catalog which will belong to service
	for _, descriptor := range globalCatalog {
		if itype == descriptor.StatType {
			if len(descriptor.ValueNames) > 0 {
				for _, suffix := range descriptor.ValueNames {
					var metricName = descriptor.Name + "_" + suffix
					e[metricName] = prometheus.NewDesc(metricName, globalHelpCatalog[metricName], descriptor.LabelNames, prometheus.Labels{"project_id": projectid, "sid": sid, "db_instance": hostname})
				}
			} else {
				e[descriptor.Name] = prometheus.NewDesc(descriptor.Name, globalHelpCatalog[descriptor.Name], descriptor.LabelNames, prometheus.Labels{"project_id": projectid, "sid": sid, "db_instance": hostname})
			}
			descriptor.Active = true // TODO: есть отдельный метод для активации, надо переделать на него
			localCatalog = append(localCatalog, descriptor)
		}
	}

	return &prometheusExporter{ServiceID: sid, AllDesc: e, ServiceRepo: repo, statCatalog: localCatalog}, nil
}

// Describe method describes all metrics specified in the exporter
func (e *prometheusExporter) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range e.AllDesc {
		ch <- desc
	}
}

// Collect method collects all metrics specified in the exporter
func (e *prometheusExporter) Collect(ch chan<- prometheus.Metric) {
	var metricsCnt int
	var servicesIDs = e.ServiceRepo.getServiceIDs()

	for _, id := range servicesIDs {
		var service = e.ServiceRepo.getService(id)
		// depending on service type run a specific collecting function
		if e.ServiceID == service.ServiceID {
			switch service.ConnSettings.ServiceType {
			case model.ServiceTypePostgresql:
				metricsCnt += e.collectPostgresMetrics(ch, service)
			case model.ServiceTypePgbouncer:
				metricsCnt += e.collectPgbouncerMetrics(ch, service)
			case model.ServiceTypeSystem:
				metricsCnt += e.collectSystemMetrics(ch)
			}

			// check total number of failures, if too many errors then unregister exporter
			if e.totalFailed >= exporterFailureLimit {
				prometheus.Unregister(e)
				e.ServiceRepo.removeServiceByServiceID(service.ServiceID)
				log.Warnln("service has been removed from the repo, too many collect failures")
			}
			log.Debugf("%s: %d metrics generated", service.ServiceID, metricsCnt)
		}
	}
}

// stringsFind returns true if array of strings contains specific string
func stringsContains(ss []string, s string) bool {
	for _, val := range ss {
		if val == s {
			return true
		}
	}
	return false
}
