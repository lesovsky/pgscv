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

// AuthConfig defines configuration settings for authentication.
type AuthConfig struct {
	EnableAuth bool   // flag tells about authentication should be enabled
	Username   string `yaml:"username"` // username used for basic authentication
	Password   string `yaml:"password"` // password used for basic authentication
	EnableTLS  bool   // flag tells about TLS should be enabled
	Keyfile    string `yaml:"keyfile"`  // path to key file
	Certfile   string `yaml:"certfile"` // path to certificate file
}

// Validate check authentication options of AuthConfig and returns toggle flags.
func (cfg AuthConfig) Validate() (bool, bool, error) {
	var enableAuth, enableTLS bool

	if (cfg.Username == "" && cfg.Password != "") || (cfg.Username != "" && cfg.Password == "") {
		return false, false, fmt.Errorf("authentication settings invalid")
	}

	if (cfg.Keyfile == "" && cfg.Certfile != "") || (cfg.Keyfile != "" && cfg.Certfile == "") {
		return false, false, fmt.Errorf("TLS settings invalid")
	}

	if cfg.Username != "" && cfg.Password != "" {
		enableAuth = true
	}

	if cfg.Keyfile != "" && cfg.Certfile != "" {
		enableTLS = true
	}

	return enableAuth, enableTLS, nil
}

// ServerConfig defines HTTP server configuration.
type ServerConfig struct {
	Addr string
	AuthConfig
}

// Server defines HTTP server.
type Server struct {
	config ServerConfig
	server *http.Server
}

// NewServer creates new HTTP server instance.
func NewServer(cfg ServerConfig) *Server {
	mux := http.NewServeMux()

	mux.Handle("/", handleRoot())

	if cfg.EnableAuth {
		mux.Handle("/metrics", basicAuth(cfg.AuthConfig, promhttp.Handler()))
	} else {
		mux.Handle("/metrics", promhttp.Handler())
	}

	return &Server{
		config: cfg,
		server: &http.Server{
			Addr:         cfg.Addr,
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

// basicAuth is a middleware for basic authentication.
func basicAuth(cfg AuthConfig, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if ok {
			if username == cfg.Username && password == cfg.Password {
				next.ServeHTTP(w, r)
				return
			}
		}

		w.Header().Set("WWW-Authenticate", `Basic realm="restricted", charset="UTF-8"`)
		http.Error(w, "Unauthorized", StatusUnauthorized)
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
