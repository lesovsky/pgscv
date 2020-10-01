package collector

import (
	"context"
	"github.com/barcodepro/pgscv/internal/store"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
	"time"
)

func Test_runTailLoop(t *testing.T) {
	c, err := NewPostgresLogsCollector(nil)
	assert.NoError(t, err)
	assert.NotNil(t, c)
	lc := c.(*postgresLogsCollector)

	fname1 := "/tmp/pgscv_postgres_logs_test_sample_1.log"
	fstr1 := "2020-09-30 14:26:29.777 +05 797922 LOG: PID 0 in cancel request did not match any process\n"
	fname2 := "/tmp/pgscv_postgres_logs_test_sample_2.log"
	fstr2 := "2020-09-30 14:26:29.784 +05 797923 ERROR: syntax error\n"

	// create test files
	for _, name := range []string{fname1, fname2} {
		f, err := os.Create(name)
		assert.NoError(t, err)
		err = f.Close()
		assert.NoError(t, err)
	}

	// tail first file
	lc.updateLogfile <- fname1
	time.Sleep(200 * time.Millisecond)

	// write to first file
	f, err := os.OpenFile(fname1, os.O_RDWR|os.O_APPEND, 0644)
	assert.NoError(t, err)
	_, err = f.WriteString(fstr1)
	assert.NoError(t, err)
	assert.NoError(t, f.Sync())
	assert.NoError(t, f.Close())
	time.Sleep(200 * time.Millisecond)

	// tail second file
	lc.updateLogfile <- fname2
	time.Sleep(200 * time.Millisecond)

	// write to second file
	f, err = os.OpenFile(fname2, os.O_RDWR|os.O_APPEND, 0644)
	assert.NoError(t, err)
	_, err = f.WriteString(fstr2)
	assert.NoError(t, err)
	assert.NoError(t, f.Sync())
	assert.NoError(t, f.Close())
	time.Sleep(200 * time.Millisecond)

	// tail first file again
	lc.updateLogfile <- fname1
	time.Sleep(200 * time.Millisecond)

	// write to first file
	f, err = os.OpenFile(fname1, os.O_RDWR|os.O_APPEND, 0644)
	assert.NoError(t, err)
	_, err = f.WriteString(fstr1)
	assert.NoError(t, err)
	assert.NoError(t, f.Sync())
	assert.NoError(t, f.Close())
	time.Sleep(200 * time.Millisecond)

	// check store content
	lc.totals.mu.RLock()
	assert.Equal(t, float64(2), lc.totals.store["log"])
	assert.Equal(t, float64(1), lc.totals.store["error"])
	lc.totals.mu.RUnlock()

	// remove test files
	for _, name := range []string{fname1, fname2} {
		assert.NoError(t, os.Remove(name))
	}
}

func Test_tailCollect(t *testing.T) {
	c, err := NewPostgresLogsCollector(nil)
	assert.NoError(t, err)
	assert.NotNil(t, c)
	lc := c.(*postgresLogsCollector)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	tailCollect(ctx, "testdata/datadir/postgresql.log.golden", false, &wg, lc)
	assert.Equal(t, float64(6), lc.totals.store["log"])
	assert.Equal(t, float64(1), lc.totals.store["error"])
	assert.Equal(t, float64(2), lc.totals.store["fatal"])

	wg.Wait()
}

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

func Test_logParser_parseLogMessage(t *testing.T) {
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
		got, ok := p.parseLogMessage(tc.line)
		assert.Equal(t, tc.want, got)
		assert.Equal(t, tc.found, ok)
	}
}
