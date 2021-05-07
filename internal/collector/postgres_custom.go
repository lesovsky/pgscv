package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"strings"
)

type postgresCustomCollector struct {
	descSets []typedDescSet
}

// NewPostgresCustomCollector returns a new Collector that expose user-defined postgres metrics.
func NewPostgresCustomCollector(constLabels prometheus.Labels, settings model.CollectorSettings) (Collector, error) {
	var sets []typedDescSet

	// Iterate over all passed subsystems and create dedicated descs set per each subsystem.
	// Consider all metrics are in the 'postgres' namespace.
	for k, v := range settings.Subsystems {
		descset := newDescSet(constLabels, "postgres", k, v)
		sets = append(sets, descset)
	}

	return &postgresCustomCollector{
		descSets: sets,
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresCustomCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Iterate over descs sets. Every set contains metrics and labels names, query used
	// for getting data and metrics descriptors. All these sufficient to request stats
	// and translate stats into metrics.

	for _, s := range c.descSets {
		res, err := conn.Query(s.query)
		if err != nil {
			log.Errorf("query failed: %s; skip", err)
			continue
		}

		stats := parsePostgresCustomStats(res, s.variableLabels)

		// iterate over stats, extract labels and values, wrap to metric and send to receiver.
		for key, stat := range stats {
			labelValues := strings.Split(key, "/")

			for name, value := range stat {
				d := s.descs[name]
				ch <- d.mustNewConstMetric(value, labelValues...)
			}
		}
	}

	return nil
}
