package http

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/weaponry/pgscv/internal/log"
	"io"
	"net/http"
	"time"
)

// Server defines HTTP server.
type Server struct {
	server *http.Server
}

// NewServer creates new HTTP server instance.
func NewServer(addr string) *Server {
	mux := http.NewServeMux()

	mux.Handle("/", handleRoot())
	mux.Handle("/metrics", promhttp.Handler())

	return &Server{
		server: &http.Server{
			Addr:         addr,
			Handler:      mux,
			IdleTimeout:  10 * time.Second,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 30 * time.Second,
		},
	}
}

// Serve method starts listening and serving requests.
func (s *Server) Serve() error {
	log.Infof("listen on %s", s.server.Addr)

	return s.server.ListenAndServe()
}

// handleRoot defines handler for '/' endpoint.
func handleRoot() http.Handler {
	const htmlTemplate = `<html>
<head><title>pgSCV / Weaponry metrics collector</title></head>
<body>
pgSCV / <a href="https://weaponry.io">Weaponry</a> metrics collector, for more info visit <a href="https://github.com/weaponry/pgscv">Github</a> page.
<p><a href="/metrics">Metrics</a></p>
</body>
</html>
`

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte(htmlTemplate))
		if err != nil {
			log.Warnln("response write failed: ", err)
		}
	})
}

// NewPushRequest creates new HTTP request for sending metrics into remote service.
func NewPushRequest(url, apiKey, hostname string, payload []byte) (*http.Request, error) {
	req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/text")
	req.Header.Set("User-Agent", "pgSCV")
	req.Header.Add("X-Weaponry-Api-Key", apiKey)

	q := req.URL.Query()
	q.Add("timestamp", fmt.Sprintf("%d", time.Now().UnixNano()/1000000))
	q.Add("extra_label", fmt.Sprintf("instance=%s", hostname))
	req.URL.RawQuery = q.Encode()

	return req, nil
}

// DoPushRequest sends prepared request with metrics into remote service.
func DoPushRequest(cl *Client, req *http.Request) error {
	log.Debugln("send metrics")

	resp, err := cl.Do(req)
	if err != nil {
		return fmt.Errorf("send failed: %s", err)
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(resp.Body, 512))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		return fmt.Errorf("send failed: %s (%s)", resp.Status, line)
	}

	log.Debugf("send success: %s", resp.Status)

	return nil
}
