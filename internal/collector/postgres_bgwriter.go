package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"strconv"
)

const (
	postgresBgwriterQuery = "SELECT " +
		"checkpoints_timed, checkpoints_req, checkpoint_write_time, checkpoint_sync_time, " +
		"buffers_checkpoint, buffers_clean, maxwritten_clean, " +
		"buffers_backend, buffers_backend_fsync, buffers_alloc, " +
		"coalesce(extract('epoch' from age(now(), stats_reset)), 0) as stats_age_seconds " +
		"FROM pg_stat_bgwriter"
)

type postgresBgwriterCollector struct {
	descs map[string]typedDesc
}

// NewPostgresBgwriterCollector returns a new Collector exposing postgres bgwriter and checkpointer stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-BGWRITER-VIEW
func NewPostgresBgwriterCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &postgresBgwriterCollector{
		descs: map[string]typedDesc{
			"checkpoints": newBuiltinTypedDesc(
				descOpts{"postgres", "checkpoints", "total", "Total number of checkpoints that have been performed of each type.", 0},
				prometheus.CounterValue,
				[]string{"checkpoint"}, constLabels,
				settings.Filters,
			),
			"checkpoints_all": newBuiltinTypedDesc(
				descOpts{"postgres", "checkpoints", "all_total", "Total number of checkpoints that have been performed.", 0},
				prometheus.CounterValue,
				nil, constLabels,
				settings.Filters,
			),
			"checkpoint_time": newBuiltinTypedDesc(
				descOpts{"postgres", "checkpoints", "seconds_total", "Total amount of time that has been spent processing data during checkpoint in each stage, in seconds.", .001},
				prometheus.CounterValue,
				[]string{"stage"}, constLabels,
				settings.Filters,
			),
			"checkpoint_time_all": newBuiltinTypedDesc(
				descOpts{"postgres", "checkpoints", "seconds_all_total", "Total amount of time that has been spent processing data during checkpoint, in seconds.", .001},
				prometheus.CounterValue,
				nil, constLabels,
				settings.Filters,
			),
			"written_bytes": newBuiltinTypedDesc(
				descOpts{"postgres", "written", "bytes_total", "Total number of bytes written by each subsystem, in bytes.", 0},
				prometheus.CounterValue,
				[]string{"process"}, constLabels,
				settings.Filters,
			),
			"maxwritten_clean": newBuiltinTypedDesc(
				descOpts{"postgres", "bgwriter", "maxwritten_clean_total", "Total number of times the background writer stopped a cleaning scan because it had written too many buffers.", 0},
				prometheus.CounterValue,
				nil, constLabels,
				settings.Filters,
			),
			"buffers_backend_fsync": newBuiltinTypedDesc(
				descOpts{"postgres", "backends", "fsync_total", "Total number of times a backends had to execute its own fsync() call.", 0},
				prometheus.CounterValue,
				nil, constLabels,
				settings.Filters,
			),
			"alloc_bytes": newBuiltinTypedDesc(
				descOpts{"postgres", "backends", "allocated_bytes_total", "Total number of bytes allocated by backends.", 0},
				prometheus.CounterValue,
				nil, constLabels,
				settings.Filters,
			),
			"stats_age_seconds": newBuiltinTypedDesc(
				descOpts{"postgres", "bgwriter", "stats_age_seconds_total", "The age of the background writer activity statistics, in seconds.", 0},
				prometheus.CounterValue,
				nil, constLabels,
				settings.Filters,
			),
		},
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresBgwriterCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	res, err := conn.Query(postgresBgwriterQuery)
	if err != nil {
		return err
	}

	stats := parsePostgresBgwriterStats(res)
	blockSize := float64(config.blockSize)

	for name, desc := range c.descs {
		switch name {
		case "checkpoints":
			ch <- desc.newConstMetric(stats.ckptTimed, "timed")
			ch <- desc.newConstMetric(stats.ckptReq, "req")
		case "checkpoints_all":
			ch <- desc.newConstMetric(stats.ckptTimed + stats.ckptReq)
		case "checkpoint_time":
			ch <- desc.newConstMetric(stats.ckptWriteTime, "write")
			ch <- desc.newConstMetric(stats.ckptSyncTime, "sync")
		case "checkpoint_time_all":
			ch <- desc.newConstMetric(stats.ckptWriteTime + stats.ckptSyncTime)
		case "maxwritten_clean":
			ch <- desc.newConstMetric(stats.bgwrMaxWritten)
		case "written_bytes":
			ch <- desc.newConstMetric(stats.ckptBuffers*blockSize, "checkpointer")
			ch <- desc.newConstMetric(stats.bgwrBuffers*blockSize, "bgwriter")
			ch <- desc.newConstMetric(stats.backendBuffers*blockSize, "backend")
		case "buffers_backend_fsync":
			ch <- desc.newConstMetric(stats.backendFsync)
		case "alloc_bytes":
			ch <- desc.newConstMetric(stats.backendAllocated * blockSize)
		case "stats_age_seconds":
			ch <- desc.newConstMetric(stats.statsAgeSeconds)
		default:
			log.Debugf("unknown desc name: %s, skip", name)
			continue
		}
	}

	return nil
}

// postgresBgwriterStat describes stats related to Postgres background writes.
type postgresBgwriterStat struct {
	ckptTimed        float64
	ckptReq          float64
	ckptWriteTime    float64
	ckptSyncTime     float64
	ckptBuffers      float64
	bgwrBuffers      float64
	bgwrMaxWritten   float64
	backendBuffers   float64
	backendFsync     float64
	backendAllocated float64
	statsAgeSeconds  float64
}

// parsePostgresBgwriterStats parses PGResult and returns struct with data values
func parsePostgresBgwriterStats(r *model.PGResult) postgresBgwriterStat {
	log.Debug("parse postgres bgwriter/checkpointer stats")

	var stats postgresBgwriterStat

	for _, row := range r.Rows {
		for i, colname := range r.Colnames {
			// Skip empty (NULL) values.
			if !row[i].Valid {
				continue
			}

			// Get data value and convert it to float64 used by Prometheus.
			v, err := strconv.ParseFloat(row[i].String, 64)
			if err != nil {
				log.Errorf("invalid input, parse '%s' failed: %s; skip", row[i].String, err)
				continue
			}

			// Update stats struct
			switch string(colname.Name) {
			case "checkpoints_timed":
				stats.ckptTimed = v
			case "checkpoints_req":
				stats.ckptReq = v
			case "checkpoint_write_time":
				stats.ckptWriteTime = v
			case "checkpoint_sync_time":
				stats.ckptSyncTime = v
			case "buffers_checkpoint":
				stats.ckptBuffers = v
			case "buffers_clean":
				stats.bgwrBuffers = v
			case "maxwritten_clean":
				stats.bgwrMaxWritten = v
			case "buffers_backend":
				stats.backendBuffers = v
			case "buffers_backend_fsync":
				stats.backendFsync = v
			case "buffers_alloc":
				stats.backendAllocated = v
			case "stats_age_seconds":
				stats.statsAgeSeconds = v
			default:
				continue
			}
		}
	}

	return stats
}
