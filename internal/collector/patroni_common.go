package collector

import (
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaponry/pgscv/internal/http"
	"github.com/weaponry/pgscv/internal/model"
	"io"
	"strings"
	"time"
)

type patroniCommonCollector struct {
	client     *http.Client
	up         typedDesc
	role       typedDesc
	version    typedDesc
	timeline   typedDesc
	changetime typedDesc
}

// NewPatroniCommonCollector returns a new Collector exposing Patroni common info.
// For details see https://patroni.readthedocs.io/en/latest/rest_api.html#monitoring-endpoint
func NewPatroniCommonCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &patroniCommonCollector{
		client: http.NewClient(http.ClientConfig{Timeout: time.Second}),
		up: newBuiltinTypedDesc(
			descOpts{"patroni", "", "up", "State of Patroni service: 0 is down, 1 is up.", 0},
			prometheus.GaugeValue,
			nil, constLabels,
			settings.Filters,
		),
		role: newBuiltinTypedDesc(
			descOpts{"patroni", "service", "role", "Labeled information about Patroni cluster role.", 0},
			prometheus.GaugeValue,
			[]string{"scope", "role"}, constLabels,
			settings.Filters,
		),
		version: newBuiltinTypedDesc(
			descOpts{"patroni", "service", "version", "Labeled information about Patroni version.", 0},
			prometheus.GaugeValue,
			[]string{"scope", "version"}, constLabels,
			settings.Filters,
		),
		timeline: newBuiltinTypedDesc(
			descOpts{"patroni", "", "current_timeline", "Current value of Postgres timeline.", 0},
			prometheus.CounterValue,
			[]string{"scope"}, constLabels,
			settings.Filters,
		),
		changetime: newBuiltinTypedDesc(
			descOpts{"patroni", "last_timeline", "change_seconds", "Latest timeline change time, in unixtime.", 0},
			prometheus.CounterValue,
			[]string{"scope"}, constLabels,
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
	info, err := requestApiPatroni(c.client, config.BaseURL)
	if err != nil {
		return err
	}

	// Request and parse history.
	resp, err := requestApiHistory(c.client, config.BaseURL)
	if err != nil {
		return err
	}

	history, err := parseHistoryResponse(resp)
	if err != nil {
		return err
	}

	ch <- c.role.newConstMetric(1, info.Patroni.Scope, info.Role)
	ch <- c.version.newConstMetric(1, info.Patroni.Scope, info.Patroni.Version)
	ch <- c.timeline.newConstMetric(float64(info.Timeline), info.Patroni.Scope)
	ch <- c.changetime.newConstMetric(history.lastTimelineChangeUnix, info.Patroni.Scope)

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
type patroniInfo struct {
	Version string `yaml:"version"`
	Scope   string `yaml:"scope"`
}

// patroniXlogInfo implements 'xlog' object of API response.
type patroniXlogInfo struct {
	Location          int64  `yaml:"location"`           // master only
	ReceivedLocation  int64  `yaml:"received_location"`  // standby only
	ReplayedLocation  int64  `yaml:"replayed_location"`  // standby only
	ReplayedTimestamp string `yaml:"replayed_timestamp"` // standby only
	Paused            bool   `yaml:"paused"`
}

// apiPatroniResponse implements API response returned by '/patroni' endpoint.
type apiPatroniResponse struct {
	State    string          `yaml:"state"`
	Role     string          `yaml:"role"`
	Timeline int             `yaml:"timeline"`
	Patroni  patroniInfo     `yaml:"patroni"`
	Xlog     patroniXlogInfo `yaml:"xlog"`
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
