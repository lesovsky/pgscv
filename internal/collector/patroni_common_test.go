package collector

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

func Test_requestApiLiveness(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	c := &http.Client{}

	err := requestApiLiveness(c, ts.URL)
	assert.NoError(t, err)

	// Test errors
	err = requestApiLiveness(c, "http://[")
	assert.Error(t, err)
	fmt.Println(err)
}

func Test_requestApiPatroni(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintln(w, `{"database_system_identifier": "6978740738459312158", "xlog": {"location": 67111576}, "timeline": 1, "server_version": 100016, "replication": [{"sync_state": "async", "usename": "replicator", "sync_priority": 0, "client_addr": "172.21.0.2", "application_name": "patroni1", "state": "streaming"}, {"sync_state": "async", "usename": "replicator", "sync_priority": 0, "client_addr": "172.21.0.6", "application_name": "patroni2", "state": "streaming"}], "postmaster_start_time": "2021-06-28 07:18:44.565317+00:00", "patroni": {"version": "2.0.2", "scope": "demo"}, "state": "running", "cluster_unlocked": false, "role": "master"}`)
	}))
	defer ts.Close()

	c := &http.Client{}

	got, err := requestApiPatroni(c, ts.URL)
	assert.NoError(t, err)
	assert.Equal(t, &apiPatroniResponse{
		State:    "running",
		Role:     "master",
		Timeline: 1,
		Patroni: patroniInfo{
			Version: "2.0.2",
			Scope:   "demo",
		},
		Xlog: patroniXlogInfo{
			Location:          67111576,
			ReceivedLocation:  0,
			ReplayedLocation:  0,
			ReplayedTimestamp: "",
			Paused:            false,
		},
	}, got)

	// Test errors
	_, err = requestApiPatroni(c, "http://127.0.0.1:30080/invalid")
	assert.Error(t, err)
}

func Test_requestApiHistory(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintln(w, `[[1, 1234, "no recovery target specified", "2021-06-30T00:00:00.123456+00:00"],[2, 2345, "no recovery target specified", "2021-06-30T10:00:00+00:00"]]`)
	}))
	defer ts.Close()

	c := &http.Client{}

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
