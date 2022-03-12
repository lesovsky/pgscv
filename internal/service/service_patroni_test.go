package service

import (
	"github.com/lesovsky/pgscv/internal/model"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func Test_discoverPatroni(t *testing.T) {
	testcases := []struct {
		valid   bool
		cmdline string
	}{
		{valid: true, cmdline: "python2 /patroni.py testdata/patroni/patroni.golden.yml"},
		{valid: false, cmdline: ""},
		{valid: false, cmdline: "python2 /patroni.py"},
		{valid: false, cmdline: "python2 /patroni.py testdata/patroni/patroni.unknown.yml"},
	}

	cwd, err := os.Getwd()
	assert.NoError(t, err)

	for _, tc := range testcases {
		got, skip, err := discoverPatroni(1, tc.cmdline, cwd)
		if tc.valid {
			assert.NoError(t, err)
			assert.False(t, skip)
			assert.Equal(t, model.ServiceTypePatroni, got.ConnSettings.ServiceType)
		} else {
			assert.Error(t, err)
			assert.True(t, skip)
		}

		//fmt.Println(s)
	}
}

func Test_parsePatroniCmdline(t *testing.T) {
	testcases := []struct {
		cmdline string
		cwd     string
		want    string
	}{
		{cmdline: "python /patroni.py /etc/patroni/patroni.yml", want: "/etc/patroni/patroni.yml"},
		{cmdline: "python /patroni.py /etc/patroni/patroni.yaml", want: "/etc/patroni/patroni.yaml"},
		{cmdline: "python /patroni.py ./patroni.yaml", cwd: "/home/postgres", want: "/home/postgres/patroni.yaml"},
		{cmdline: "python3 /patroni.py patroni.yaml", cwd: "/home/postgres", want: "/home/postgres/patroni.yaml"},
		{cmdline: "python3 /patroni.py patroni.yaml", cwd: "", want: "/patroni.yaml"},
		{cmdline: "python3 /patroni.py", cwd: "", want: ""},
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.want, parsePatroniCmdline(tc.cmdline, tc.cwd))
	}
}

func Test_newPatroniConninfo(t *testing.T) {
	testcases := []struct {
		config string
		want   patroniConninfo
	}{
		{
			config: "testdata/patroni/patroni.golden.yml",
			want:   patroniConninfo{host: "127.0.0.1", port: "8008", ssl: true},
		},
	}
	for _, tc := range testcases {
		got, err := newPatroniConninfo(tc.config)
		assert.NoError(t, err)
		assert.Equal(t, tc.want, got)
	}
}

func Test_parseListenString(t *testing.T) {
	testcases := []struct {
		valid    bool
		str      string
		wantAddr string
		wantPort string
	}{
		{valid: false, str: ""},
		{valid: true, str: "1.2.3.4:8008", wantAddr: "1.2.3.4", wantPort: "8008"},
		{valid: true, str: "1.2.3.4", wantAddr: "1.2.3.4", wantPort: "8008"},
		{valid: true, str: "0.0.0.0:8008", wantAddr: "127.0.0.1", wantPort: "8008"},
		{valid: true, str: "0.0.0.0", wantAddr: "127.0.0.1", wantPort: "8008"},
		{valid: true, str: ":::8008", wantAddr: "[::1]", wantPort: "8008"},
		{valid: true, str: "::", wantAddr: "[::1]", wantPort: "8008"},
		{valid: true, str: "2a04:4a00:5:10a3::db04:2:8008", wantAddr: "[2a04:4a00:5:10a3::db04:2]", wantPort: "8008"},
		//{valid: true, str: "2a04:4a00:5:10a3::db04:2", wantAddr: "[2a04:4a00:5:10a3::db04:2]", wantPort: "8008"},
	}
	for _, tc := range testcases {
		gotAddr, gotPort, err := parseListenString(tc.str)
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.wantAddr, gotAddr)
			assert.Equal(t, tc.wantPort, gotPort)
		} else {
			assert.Error(t, err)
		}

	}
}
