package http

import (
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync"
	"testing"
	"time"
)

func TestServer_Serve(t *testing.T) {
	addr := "127.0.0.1:17890"
	srv := NewServer(addr)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err := srv.Serve()
		assert.NoError(t, err)
		wg.Done()
	}()

	time.Sleep(100 * time.Millisecond)

	cl := NewClient(ClientConfig{})
	endpoints := []string{"/", "/metrics"}

	for _, e := range endpoints {
		_, err := cl.Get("http://" + addr + e)
		assert.NoError(t, err)
	}
}

func Test_handleRoot(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	res := httptest.NewRecorder()

	mux := http.NewServeMux()
	mux.Handle("/", handleRoot())
	mux.ServeHTTP(res, req)

	assert.Equal(t, StatusOK, res.Code)

	body, err := io.ReadAll(res.Body)
	assert.NoError(t, err)
	assert.Contains(t, string(body), `pgSCV / <a href="https://weaponry.io">Weaponry</a> metrics collector, for more info visit <a href="https://github.com/weaponry/pgscv">Github</a> page.`)
	res.Flush()
}

func TestNewPushRequest(t *testing.T) {
	req, err := NewPushRequest("https://example.org", "example", "example", []byte("example"))
	assert.NoError(t, err)

	assert.Equal(t, "pgSCV", req.Header.Get("User-Agent"))
	assert.Equal(t, "example", req.Header.Get("X-Weaponry-Api-Key"))

	re := regexp.MustCompile(`^https://example.org\?extra_label=instance%3Dexample&timestamp=[0-9]{13}$`)
	assert.True(t, re.MatchString(req.URL.String()))

	// test with invalid url
	_, err = NewPushRequest("https://[[", "example", "example", []byte("example"))
	assert.Error(t, err)
}

func TestDoPushRequest(t *testing.T) {
	ts := TestServer(t, StatusOK, "")
	defer ts.Close()

	ts2 := TestServer(t, StatusBadRequest, "invalid data")
	defer ts2.Close()

	cl := NewClient(ClientConfig{})

	req, err := NewPushRequest(ts.URL, "example", "example", []byte("example"))
	assert.NoError(t, err)
	assert.NoError(t, DoPushRequest(cl, req))

	req, err = NewPushRequest(ts2.URL, "example", "example", []byte("example"))
	assert.NoError(t, err)
	assert.Error(t, DoPushRequest(cl, req))
}
