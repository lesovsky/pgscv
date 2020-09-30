package collector

import (
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_queryCurrentLogfile(t *testing.T) {
	got, err := queryCurrentLogfile(store.TestPostgresConnStr)
	assert.NoError(t, err)
	assert.NotEqual(t, got, "")

	got, err = queryCurrentLogfile("host=127.0.0.1 port=1 user=invalid dbname=invalid")
	assert.Error(t, err)
	assert.Equal(t, got, "")
}

func Test_newLogParser(t *testing.T) {
	p := newLogParser()
	assert.NotNil(t, p)
	assert.Greater(t, len(p.re), 0)
}

func Test_logParser_parse(t *testing.T) {
	testcases := []struct {
		line  string
		want  string
		found bool
	}{
		{line: "2020-09-29 14:08:52.858 +05 1060 [] LOG: test", want: "log", found: true},
		{line: "2020-09-29 14:08:52.858 +05 1060 [] WARNING: test", want: "warning", found: true},
		{line: "2020-09-29 14:08:52.858 +05 1060 [] ERROR: test", want: "error", found: true},
		{line: "2020-09-29 14:08:52.858 +05 1060 [] FATAL: test", want: "fatal", found: true},
		{line: "2020-09-29 14:08:52.858 +05 1060 [] PANIC: test", want: "panic", found: true},
		{line: "", want: "", found: false},
		{line: "test", want: "", found: false},
	}

	p := newLogParser()

	for _, tc := range testcases {
		got, ok := p.parse(tc.line)
		assert.Equal(t, tc.want, got)
		assert.Equal(t, tc.found, ok)
	}
}
