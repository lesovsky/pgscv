package collector

import (
	"encoding/json"
	"fmt"
	"github.com/lesovsky/pgscv/internal/http"
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"strconv"
	"strings"
	"time"
)

type patroniCommonCollector struct {
	client            *http.Client
	up                typedDesc
	version           typedDesc
	pgup              typedDesc
	pgstart           typedDesc
	roleMaster        typedDesc
	roleStandbyLeader typedDesc
	roleReplica       typedDesc
	xlogLoc           typedDesc
	xlogRecvLoc       typedDesc
	xlogReplLoc       typedDesc
	xlogReplTs        typedDesc
	xlogPaused        typedDesc
	pgversion         typedDesc
	unlocked          typedDesc
	timeline          typedDesc
	changetime        typedDesc
}

// NewPatroniCommonCollector returns a new Collector exposing Patroni common info.
// For details see https://patroni.readthedocs.io/en/latest/rest_api.html#monitoring-endpoint
func NewPatroniCommonCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	varLabels := []string{"scope"}

	return &patroniCommonCollector{
		client: http.NewClient(http.ClientConfig{Timeout: time.Second}),
		up: newBuiltinTypedDesc(
			descOpts{"patroni", "", "up", "State of Patroni service: 1 is up, 0 otherwise.", 0},
			prometheus.GaugeValue,
			nil, constLabels,
			settings.Filters,
		),
		version: newBuiltinTypedDesc(
			descOpts{"patroni", "", "version", "Numeric representation of Patroni version.", 0},
			prometheus.GaugeValue,
			[]string{"scope", "version"}, constLabels,
			settings.Filters,
		),
		pgup: newBuiltinTypedDesc(
			descOpts{"patroni", "postgres", "running", "State of Postgres service: 1 is up, 0 otherwise.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		pgstart: newBuiltinTypedDesc(
			descOpts{"patroni", "postmaster", "start_time", "Epoch seconds since Postgres started.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		roleMaster: newBuiltinTypedDesc(
			descOpts{"patroni", "", "master", "Value is 1 if this node is the leader, 0 otherwise.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		roleStandbyLeader: newBuiltinTypedDesc(
			descOpts{"patroni", "", "standby_leader", "Value is 1 if this node is the standby_leader, 0 otherwise.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		roleReplica: newBuiltinTypedDesc(
			descOpts{"patroni", "", "replica", "Value is 1 if this node is a replica, 0 otherwise.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		xlogLoc: newBuiltinTypedDesc(
			descOpts{"patroni", "xlog", "location", "Current location of the Postgres transaction log, 0 if this node is a replica.", 0},
			prometheus.CounterValue,
			varLabels, constLabels,
			settings.Filters,
		),
		xlogRecvLoc: newBuiltinTypedDesc(
			descOpts{"patroni", "xlog", "received_location", "Current location of the received Postgres transaction log, 0 if this node is the leader.", 0},
			prometheus.CounterValue,
			varLabels, constLabels,
			settings.Filters,
		),
		xlogReplLoc: newBuiltinTypedDesc(
			descOpts{"patroni", "xlog", "replayed_location", "Current location of the replayed Postgres transaction log, 0 if this node is the leader.", 0},
			prometheus.CounterValue,
			varLabels, constLabels,
			settings.Filters,
		),
		xlogReplTs: newBuiltinTypedDesc(
			descOpts{"patroni", "xlog", "replayed_timestamp", "Current timestamp of the replayed Postgres transaction log, 0 if null.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		xlogPaused: newBuiltinTypedDesc(
			descOpts{"patroni", "xlog", "paused", "Value is 1 if the replaying of Postgres transaction log is paused, 0 otherwise.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		pgversion: newBuiltinTypedDesc(
			descOpts{"patroni", "postgres", "server_version", "Version of Postgres (if running), 0 otherwise.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		unlocked: newBuiltinTypedDesc(
			descOpts{"patroni", "cluster", "unlocked", "Value is 1 if the cluster is unlocked, 0 if locked.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),

		timeline: newBuiltinTypedDesc(
			descOpts{"patroni", "postgres", "timeline", "Postgres timeline of this node (if running), 0 otherwise.", 0},
			prometheus.CounterValue,
			varLabels, constLabels,
			settings.Filters,
		),
		changetime: newBuiltinTypedDesc(
			descOpts{"patroni", "last_timeline", "change_seconds", "Epoch seconds since latest timeline switched.", 0},
			prometheus.CounterValue,
			varLabels, constLabels,
			settings.Filters,
		),
	}, nil
}

func (c *patroniCommonCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	if strings.HasPrefix(config.BaseURL, "https://") {
		c.client.EnableTLSInsecure()
	}

	// Check liveness.
	err := requestApiLiveness(c.client, config.BaseURL)
	if err != nil {
		ch <- c.up.newConstMetric(0)
		return err
	}

	ch <- c.up.newConstMetric(1)

	// Request general info.
	respInfo, err := requestApiPatroni(c.client, config.BaseURL)
	if err != nil {
		return err
	}

	info, err := parsePatroniResponse(respInfo)
	if err != nil {
		return err
	}

	// Request and parse history.
	respHist, err := requestApiHistory(c.client, config.BaseURL)
	if err != nil {
		return err
	}

	history, err := parseHistoryResponse(respHist)
	if err != nil {
		return err
	}

	ch <- c.version.newConstMetric(info.version, info.scope, info.versionStr)
	ch <- c.pgup.newConstMetric(info.running, info.scope)
	ch <- c.pgstart.newConstMetric(info.startTime, info.scope)

	ch <- c.roleMaster.newConstMetric(info.master, info.scope)
	ch <- c.roleStandbyLeader.newConstMetric(info.standbyLeader, info.scope)
	ch <- c.roleReplica.newConstMetric(info.replica, info.scope)

	ch <- c.xlogLoc.newConstMetric(info.xlogLoc, info.scope)
	ch <- c.xlogRecvLoc.newConstMetric(info.xlogRecvLoc, info.scope)
	ch <- c.xlogReplLoc.newConstMetric(info.xlogReplLoc, info.scope)
	ch <- c.xlogReplTs.newConstMetric(info.xlogReplTs, info.scope)
	ch <- c.xlogPaused.newConstMetric(info.xlogPaused, info.scope)

	ch <- c.pgversion.newConstMetric(info.pgversion, info.scope)
	ch <- c.unlocked.newConstMetric(info.unlocked, info.scope)
	ch <- c.timeline.newConstMetric(info.timeline, info.scope)

	ch <- c.changetime.newConstMetric(history.lastTimelineChangeUnix, info.scope)

	return nil
}

// requestApiLiveness requests to /liveness endpoint of API and returns error if failed.
func requestApiLiveness(c *http.Client, baseurl string) error {
	_, err := c.Get(baseurl + "/liveness")
	if err != nil {
		return err
	}

	return err
}

// patroniInfo implements 'patroni' object of API response.
type patroni struct {
	Version string `json:"version"`
	Scope   string `json:"scope"`
}

// patroniXlogInfo implements 'xlog' object of API response.
type patroniXlogInfo struct {
	Location          int64  `json:"location"`           // master only
	ReceivedLocation  int64  `json:"received_location"`  // standby only
	ReplayedLocation  int64  `json:"replayed_location"`  // standby only
	ReplayedTimestamp string `json:"replayed_timestamp"` // standby only
	Paused            bool   `json:"paused"`             // standby only
}

// apiPatroniResponse implements API response returned by '/patroni' endpoint.
type apiPatroniResponse struct {
	State         string          `json:"state"`
	Unlocked      bool            `json:"cluster_unlocked"`
	Timeline      int             `json:"timeline"`
	PmStartTime   string          `json:"postmaster_start_time"`
	ServerVersion int             `json:"server_version"`
	Patroni       patroni         `json:"patroni"`
	Role          string          `json:"role"`
	Xlog          patroniXlogInfo `json:"xlog"`
}

// patroniInfo implements metrics values extracted from the response of '/patroni' endpoint.
type patroniInfo struct {
	scope         string
	version       float64
	versionStr    string
	running       float64
	startTime     float64
	master        float64
	standbyLeader float64
	replica       float64
	xlogLoc       float64
	xlogRecvLoc   float64
	xlogReplLoc   float64
	xlogReplTs    float64
	xlogPaused    float64
	pgversion     float64
	unlocked      float64
	timeline      float64
}

// requestPatroniInfo requests to /patroni endpoint of API and returns parsed response.
func requestApiPatroni(c *http.Client, baseurl string) (*apiPatroniResponse, error) {
	resp, err := c.Get(baseurl + "/patroni")
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad response: %s", resp.Status)
	}

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	_ = resp.Body.Close()

	r := &apiPatroniResponse{}

	err = json.Unmarshal(content, r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// parsePatroniResponse parses info from API response and returns info object.
func parsePatroniResponse(resp *apiPatroniResponse) (*patroniInfo, error) {
	version, err := semverStringToInt(resp.Patroni.Version)
	if err != nil {
		return nil, fmt.Errorf("parse version string '%s' failed: %s", resp.Patroni.Version, err)
	}

	var running float64
	if resp.State == "running" {
		running = 1
	}

	t1, err := time.Parse("2006-01-02 15:04:05.999999Z07:00", resp.PmStartTime)
	if err != nil {
		return nil, fmt.Errorf("parse patroni postmaster_start_time string '%s' failed: %s", resp.PmStartTime, err)
	}

	var master, stdleader, replica float64
	switch resp.Role {
	case "master":
		master, stdleader, replica = 1, 0, 0
	case "standby_leader":
		master, stdleader, replica = 0, 1, 0
	case "replica":
		master, stdleader, replica = 0, 0, 1
	}

	var xlogReplTimeSecs float64
	if resp.Xlog.ReplayedTimestamp != "null" && resp.Xlog.ReplayedTimestamp != "" {
		t, err := time.Parse("2006-01-02 15:04:05.999999Z07:00", resp.Xlog.ReplayedTimestamp)
		if err != nil {
			return nil, fmt.Errorf("parse patroni xlog.replayed_timestamp string '%s' failed: %s", resp.PmStartTime, err)
		}
		xlogReplTimeSecs = float64(t.UnixNano()) / 1000000000
	}

	var paused float64
	if resp.Xlog.Paused {
		paused = 1
	}

	var unlocked float64
	if resp.Unlocked {
		unlocked = 1
	}

	return &patroniInfo{
		scope:         resp.Patroni.Scope,
		version:       float64(version),
		versionStr:    resp.Patroni.Version,
		running:       running,
		startTime:     float64(t1.UnixNano()) / 1000000000,
		master:        master,
		standbyLeader: stdleader,
		replica:       replica,
		xlogLoc:       float64(resp.Xlog.Location),
		xlogRecvLoc:   float64(resp.Xlog.ReceivedLocation),
		xlogReplLoc:   float64(resp.Xlog.ReplayedLocation),
		xlogReplTs:    xlogReplTimeSecs,
		xlogPaused:    paused,
		pgversion:     float64(resp.ServerVersion),
		unlocked:      unlocked,
		timeline:      float64(resp.Timeline),
	}, nil
}

// patroniHistoryUnit defines single item of Patroni history in the API response.
// Basically this is array like [ int, int, string, string ].
type patroniHistoryUnit []interface{}

// apiHistoryResponse defines the API response with complete history.
type apiHistoryResponse []patroniHistoryUnit

// patroniHistory describes details (UNIX timestamp and reason) of the latest timeline change.
type patroniHistory struct {
	lastTimelineChangeReason string
	lastTimelineChangeUnix   float64
}

// requestApiHistory requests /history endpoint of API and returns parsed response.
func requestApiHistory(c *http.Client, baseurl string) (apiHistoryResponse, error) {
	resp, err := c.Get(baseurl + "/history")
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad response: %s", resp.Status)
	}

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	_ = resp.Body.Close()

	r := apiHistoryResponse{}

	err = json.Unmarshal(content, &r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// parseHistoryResponse parses history and returns info about latest event in the history.
func parseHistoryResponse(resp apiHistoryResponse) (patroniHistory, error) {
	if len(resp) == 0 {
		return patroniHistory{}, nil
	}

	unit := resp[len(resp)-1]

	if len(unit) < 4 {
		return patroniHistory{}, fmt.Errorf("history unit invalid len")
	}

	// Check value types.
	reason, ok := unit[2].(string)
	if !ok {
		return patroniHistory{}, fmt.Errorf("history unit invalid message value type")
	}

	timestamp, ok := unit[3].(string)
	if !ok {
		return patroniHistory{}, fmt.Errorf("history unit invalid timestamp value type")
	}

	t, err := time.Parse("2006-01-02T15:04:05.999999Z07:00", timestamp)
	if err != nil {
		return patroniHistory{}, err
	}

	return patroniHistory{
		lastTimelineChangeReason: reason,
		lastTimelineChangeUnix:   float64(t.UnixNano()) / 1000000000,
	}, nil
}

// semverStringToInt parse valid semver version string and returns numeric representation.
func semverStringToInt(version string) (int, error) {
	// remove additional suffix in patch version if exists.
	version = strings.TrimSuffix(version, strings.TrimLeft(version, "1234567890."))

	nums := strings.Split(version, ".")
	if len(nums) < 3 {
		return 0, fmt.Errorf("invalid version string: '%s'", version)
	}

	var res string
	for i, num := range nums {
		if i > 2 {
			break
		}

		switch i {
		case 1, 2:
			if len(num) < 2 {
				num = "0" + num
			}
		}
		res = res + num
	}

	v, err := strconv.Atoi(res)
	if err != nil {
		return 0, err
	}

	return v, nil
}
