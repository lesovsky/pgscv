package collector

import (
	"fmt"
	"github.com/lesovsky/pgscv/internal/http"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_requestApiLiveness(t *testing.T) {
	ts := http.TestServer(t, http.StatusOK, "")
	defer ts.Close()

	c := http.NewClient(http.ClientConfig{})

	err := requestApiLiveness(c, ts.URL)
	assert.NoError(t, err)

	// Test errors
	err = requestApiLiveness(c, "http://[")
	assert.Error(t, err)
	fmt.Println(err)
}

func Test_requestApiPatroni(t *testing.T) {
	testcases := []struct {
		name     string
		response string
		want     *apiPatroniResponse
	}{
		{
			name:     "leader",
			response: `{"database_system_identifier": "6978740738459312158", "xlog": {"location": 67111576}, "timeline": 1, "server_version": 100016, "replication": [{"sync_state": "async", "usename": "replicator", "sync_priority": 0, "client_addr": "172.21.0.2", "application_name": "patroni1", "state": "streaming"}, {"sync_state": "async", "usename": "replicator", "sync_priority": 0, "client_addr": "172.21.0.6", "application_name": "patroni2", "state": "streaming"}], "postmaster_start_time": "2021-06-28 07:18:44.565317+00:00", "patroni": {"version": "2.0.2", "scope": "demo"}, "state": "running", "cluster_unlocked": false, "role": "master"}`,
			want: &apiPatroniResponse{
				State:         "running",
				Unlocked:      false,
				Timeline:      1,
				PmStartTime:   "2021-06-28 07:18:44.565317+00:00",
				ServerVersion: 100016,
				Patroni:       patroni{Version: "2.0.2", Scope: "demo"},
				Role:          "master",
				Xlog:          patroniXlogInfo{Location: 67111576, ReceivedLocation: 0, ReplayedLocation: 0, ReplayedTimestamp: "", Paused: false},
			},
		},
		{
			name:     "replica",
			response: `{"patroni": {"scope": "demo", "version": "2.1.0"}, "database_system_identifier": "6981836146590883870", "postmaster_start_time": "2021-07-06 15:31:03.056298+00:00", "cluster_unlocked": false, "timeline": 1, "state": "running", "server_version": 100016, "role": "replica", "xlog": {"received_location": 67211944, "replayed_timestamp": "2021-07-09 05:30:41.207477+00:00", "replayed_location": 67211944, "paused": false}}`,
			want: &apiPatroniResponse{
				State:         "running",
				Unlocked:      false,
				Timeline:      1,
				PmStartTime:   "2021-07-06 15:31:03.056298+00:00",
				ServerVersion: 100016,
				Patroni:       patroni{Version: "2.1.0", Scope: "demo"},
				Role:          "replica",
				Xlog:          patroniXlogInfo{Location: 0, ReceivedLocation: 67211944, ReplayedLocation: 67211944, ReplayedTimestamp: "2021-07-09 05:30:41.207477+00:00", Paused: false},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ts := http.TestServer(t, http.StatusOK, tc.response)
			defer ts.Close()

			c := http.NewClient(http.ClientConfig{})

			got, err := requestApiPatroni(c, ts.URL)
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	// Test errors
	t.Run("invalid url", func(t *testing.T) {
		c := http.NewClient(http.ClientConfig{})
		_, err := requestApiPatroni(c, "http://127.0.0.1:30080/invalid")
		assert.Error(t, err)
	})
}

func Test_parsePatroniResponse(t *testing.T) {
	testcases := []struct {
		valid bool
		resp  *apiPatroniResponse
		want  *patroniInfo
	}{
		{
			valid: true,
			resp: &apiPatroniResponse{
				State:         "running",
				Unlocked:      false,
				Timeline:      1,
				PmStartTime:   "2021-06-28 07:18:44.565317+00:00",
				ServerVersion: 100016,
				Patroni:       patroni{Version: "2.0.2", Scope: "demo"},
				Role:          "master",
				Xlog:          patroniXlogInfo{Location: 67111576, ReceivedLocation: 0, ReplayedLocation: 0, ReplayedTimestamp: "", Paused: false},
			},
			want: &patroniInfo{
				scope: "demo", version: 20002, versionStr: "2.0.2", running: 1, startTime: 1624864724.5653172,
				master: 1, standbyLeader: 0, replica: 0,
				xlogLoc: 67111576, xlogRecvLoc: 0, xlogReplLoc: 0, xlogReplTs: 0, xlogPaused: 0,
				pgversion: 100016, unlocked: 0, timeline: 1,
			},
		},
		{
			valid: true,
			resp: &apiPatroniResponse{
				State:         "running",
				Unlocked:      true,
				Timeline:      1,
				PmStartTime:   "2021-07-06 15:31:03.056298+00:00",
				ServerVersion: 100016,
				Patroni:       patroni{Version: "2.1.0", Scope: "demo"},
				Role:          "replica",
				Xlog:          patroniXlogInfo{Location: 0, ReceivedLocation: 67211944, ReplayedLocation: 67211944, ReplayedTimestamp: "2021-07-09 05:30:41.207477+00:00", Paused: true},
			},
			want: &patroniInfo{
				scope: "demo", version: 20100, versionStr: "2.1.0", running: 1, startTime: 1625585463.056298,
				master: 0, standbyLeader: 0, replica: 1,
				xlogLoc: 0, xlogRecvLoc: 67211944, xlogReplLoc: 67211944, xlogReplTs: 1625808641.207477, xlogPaused: 1,
				pgversion: 100016, unlocked: 1, timeline: 1,
			},
		},
		{valid: false, resp: &apiPatroniResponse{Patroni: patroni{Version: "invalid"}}},
		{valid: false, resp: &apiPatroniResponse{PmStartTime: "invalid", Patroni: patroni{Version: "1.2.0"}}},
		{
			valid: false,
			resp: &apiPatroniResponse{
				Patroni:     patroni{Version: "1.2.0"},
				PmStartTime: "2021-07-06 15:31:03.056298+00:00",
				Xlog:        patroniXlogInfo{ReplayedTimestamp: "invalid"},
			},
		},
	}

	for _, tc := range testcases {
		if tc.valid {
			got, err := parsePatroniResponse(tc.resp)
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		} else {
			_, err := parsePatroniResponse(tc.resp)
			assert.Error(t, err)
		}
	}
}

func Test_requestApiHistory(t *testing.T) {
	ts := http.TestServer(t, http.StatusOK,
		`[[1, 1234, "no recovery target specified", "2021-06-30T00:00:00.123456+00:00"],[2, 2345, "no recovery target specified", "2021-06-30T10:00:00+00:00"]]`,
	)
	defer ts.Close()

	c := http.NewClient(http.ClientConfig{})

	got, err := requestApiHistory(c, ts.URL)
	assert.NoError(t, err)
	assert.EqualValues(t, apiHistoryResponse{
		{float64(1), float64(1234), "no recovery target specified", "2021-06-30T00:00:00.123456+00:00"},
		{float64(2), float64(2345), "no recovery target specified", "2021-06-30T10:00:00+00:00"},
	}, got)

	// Test errors
	_, err = requestApiHistory(c, "http://127.0.0.1:30080/invalid")
	assert.Error(t, err)
}

func Test_parseHistoryResponse(t *testing.T) {
	testcases := []struct {
		valid bool
		resp  apiHistoryResponse
		want  patroniHistory
	}{
		{valid: true, resp: apiHistoryResponse{
			{int64(1), int64(12345678), "no recovery target specified", "2021-06-30T00:00:00+00:00"},
			{int64(2), int64(23456789), "no recovery target specified", "2021-06-30T10:00:00.123456+00:00"},
		}, want: patroniHistory{
			lastTimelineChangeUnix:   1625047200.123456,
			lastTimelineChangeReason: "no recovery target specified",
		}},
		{valid: true, resp: apiHistoryResponse{}},
		// invalid test data
		{valid: false, resp: apiHistoryResponse{{int64(1), int64(1)}}},
		{valid: false, resp: apiHistoryResponse{{int64(1), int64(1), int64(1), "example"}}},
		{valid: false, resp: apiHistoryResponse{{int64(1), int64(1), "example", int64(1)}}},
		{valid: false, resp: apiHistoryResponse{{int64(1), int64(1), "example", "invalid"}}},
	}

	for _, tc := range testcases {
		got, err := parseHistoryResponse(tc.resp)
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_semverStringToInt(t *testing.T) {
	testcases := []struct {
		valid   bool
		version string
		want    int
	}{
		{valid: true, version: "0.0.1-pre0", want: 1},
		{valid: true, version: "0.0.1", want: 1},
		{valid: true, version: "0.0.1.2", want: 1},
		{valid: true, version: "0.1.2", want: 102},
		{valid: true, version: "0.1.2-pre0", want: 102},
		{valid: true, version: "1.2.3", want: 10203},
		{valid: true, version: "1.2.3-pre0", want: 10203},
		{valid: true, version: "1.2.13", want: 10213},
		{valid: true, version: "1.2.13-pre0", want: 10213},
		{valid: true, version: "1.12.23", want: 11223},
		{valid: true, version: "1.12.23-pre0", want: 11223},
		{valid: true, version: "11.22.33", want: 112233},
		{valid: true, version: "11.22.33-pre0", want: 112233},
		{valid: false, version: "22.33"},
	}

	for _, tc := range testcases {
		got, err := semverStringToInt(tc.version)
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		} else {
			assert.Error(t, err)
		}
	}
}
