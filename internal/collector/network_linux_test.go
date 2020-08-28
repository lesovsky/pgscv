package collector

import (
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

func TestNetworkCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"node_network_addresses_total",
		},
		collector: NewNetworkCollector,
	}

	pipeline(t, input)
}

func Test_parseInterfaceAddresses(t *testing.T) {
	addresses := []net.Addr{
		&net.IPAddr{IP: net.ParseIP("10.50.20.22")},
		&net.IPAddr{IP: net.ParseIP("172.17.40.11")},
		&net.IPAddr{IP: net.ParseIP("192.168.122.1")},
		&net.IPAddr{IP: net.ParseIP("8.8.8.8")},
		&net.IPAddr{IP: net.ParseIP("8.8.4.4")},
		&net.IPAddr{IP: net.ParseIP("invalid")},
	}
	want := map[string]int{
		"public":  2,
		"private": 3,
	}

	got := parseInterfaceAddresses(addresses)
	assert.Equal(t, want, got)
}

func Test_isPrivate(t *testing.T) {
	var testcases = []struct {
		in    string
		valid bool
		want  bool
	}{
		{in: "127.0.0.1", valid: true, want: true},
		{in: "127.0.0.1/32", valid: true, want: true},
		{in: "10.20.30.40", valid: true, want: true},
		{in: "10.20.30.40/8", valid: true, want: true},
		{in: "172.16.8.4", valid: true, want: true},
		{in: "172.16.8.4/16", valid: true, want: true},
		{in: "192.168.100.55", valid: true, want: true},
		{in: "192.168.100.55/24", valid: true, want: true},
		{in: "::1", valid: true, want: true},
		{in: "fd00:3456::", valid: true, want: true},
		{in: "fe80:1234::", valid: true, want: true},
		{in: "1.2.4.5", valid: true, want: false},
		{in: "12.10.10.1", valid: true, want: false},
		{in: "180.142.250.11", valid: true, want: false},
		{in: "195.85.48.44", valid: true, want: false},
		{in: "invalid", valid: false, want: false},
		{in: "1.1", valid: false, want: false},
		{in: "1:1", valid: false, want: false},
	}

	for _, tc := range testcases {
		got, err := isPrivate(tc.in)
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		} else {
			assert.Error(t, err)
			assert.Equal(t, tc.want, got)
		}

	}
}
