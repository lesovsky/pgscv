package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"strconv"
)

const (
	postgresBgwriterQuery = `SELECT
  checkpoints_timed, checkpoints_req,
  checkpoint_write_time, checkpoint_sync_time,
  buffers_checkpoint, buffers_clean, maxwritten_clean,
  buffers_backend, buffers_backend_fsync, buffers_alloc,
  coalesce(extract('epoch' from age(now(), stats_reset)), 0) as stats_age_seconds
FROM pg_stat_bgwriter`
)

type postgresBgwriterCollector struct {
	descs map[string]typedDesc
}

// NewPostgresBgwriterCollector returns a new Collector exposing postgres bgwriter and checkpointer stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-BGWRITER-VIEW
func NewPostgresBgwriterCollector(constLabels prometheus.Labels) (Collector, error) {
	return &postgresBgwriterCollector{
		descs: map[string]typedDesc{
			"checkpoints": {
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "ckpt", "checkpoints_total"),
					"Total number of checkpoints that have been performed of each type.",
					[]string{"ckpt"}, constLabels,
				), valueType: prometheus.CounterValue,
			},
			"checkpoint_time": {
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "ckpt", "time_seconds_total"),
					"Total amount of time that has been spent processing data during checkpoint in each stage, in seconds.",
					[]string{"stage"}, constLabels,
				), valueType: prometheus.CounterValue, factor: .001,
			},
			"checkpoint_time_all": {
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "ckpt", "time_seconds_all_total"),
					"Total amount of time that has been spent processing data during checkpoint, in seconds.",
					nil, constLabels,
				), valueType: prometheus.CounterValue, factor: .001,
			},
			"written_bytes": {
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "written", "bytes_total"),
					"Total number of bytes written by each subsystem, in bytes.",
					[]string{"process"}, constLabels,
				), valueType: prometheus.CounterValue,
			},
			"maxwritten_clean": {
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "bgwriter", "maxwritten_clean_total"),
					"Total number of times the background writer stopped a cleaning scan because it had written too many buffers.",
					nil, constLabels,
				), valueType: prometheus.CounterValue,
			},
			"buffers_backend_fsync": {
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "backends", "fsync_total"),
					"Total number of times a backends had to execute its own fsync() call.",
					nil, constLabels,
				), valueType: prometheus.CounterValue,
			},
			"alloc_bytes": {
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "backends", "allocated_bytes_total"),
					"Total number of bytes allocated by backends.",
					nil, constLabels,
				), valueType: prometheus.CounterValue,
			},
			"stats_age_seconds": {
				desc: prometheus.NewDesc(
					prometheus.BuildFQName("postgres", "bgwriter", "stats_age_seconds"),
					"The age of the background writer activity statistics, in seconds.",
					nil, constLabels,
				), valueType: prometheus.CounterValue,
			},
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
	blockSize := float64(config.BlockSize)

	for name, desc := range c.descs {
		switch name {
		case "checkpoints":
			ch <- desc.mustNewConstMetric(stats.ckptTimed, "timed")
			ch <- desc.mustNewConstMetric(stats.ckptReq, "req")
		case "checkpoint_time":
			ch <- desc.mustNewConstMetric(stats.ckptWriteTime, "write")
			ch <- desc.mustNewConstMetric(stats.ckptSyncTime, "sync")
		case "checkpoint_time_all":
			ch <- desc.mustNewConstMetric(stats.ckptWriteTime + stats.ckptSyncTime)
		case "maxwritten_clean":
			ch <- desc.mustNewConstMetric(stats.bgwrMaxWritten)
		case "written_bytes":
			ch <- desc.mustNewConstMetric(stats.ckptBuffers*blockSize, "checkpointer")
			ch <- desc.mustNewConstMetric(stats.bgwrBuffers*blockSize, "bgwriter")
			ch <- desc.mustNewConstMetric(stats.backendBuffers*blockSize, "backend")
		case "buffers_backend_fsync":
			ch <- desc.mustNewConstMetric(stats.backendFsync)
		case "alloc_bytes":
			ch <- desc.mustNewConstMetric(stats.backendAllocated * blockSize)
		case "stats_age_seconds":
			ch <- desc.mustNewConstMetric(stats.statsAgeSeconds)
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
				log.Debugf("unsupported pg_stat_bgwriter stat column: %s, skip", string(colname.Name))
				continue
			}
		}
	}

	return stats
}
