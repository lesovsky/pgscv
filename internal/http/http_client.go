package http

import (
	"crypto/tls"
	"net/http"
	"time"
)

const (
	StatusOK           = http.StatusOK           // 200
	StatusBadRequest   = http.StatusBadRequest   // 400
	StatusUnauthorized = http.StatusUnauthorized // 401
	StatusNotFound     = http.StatusNotFound     // 404
)

// Client defines local wrapper on standard http.Client.
type Client struct {
	client *http.Client
}

// ClientConfig defines initial configuration when creating Client.
type ClientConfig struct {
	Timeout time.Duration
}

// NewClient creates new HTTP client.
func NewClient(cfg ClientConfig) *Client {
	const defaultTimeout = time.Second

	if cfg.Timeout == 0 {
		cfg.Timeout = defaultTimeout
	}

	return &Client{
		client: &http.Client{
			Timeout: cfg.Timeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxConnsPerHost:     10,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     120 * time.Second,
			},
		},
	}
}

// EnableTLSInsecure enables insecure TLS transport for HTTP client.
func (cl *Client) EnableTLSInsecure() {
	t := cl.client.Transport.(*http.Transport).Clone()
	t.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // #nosec G402
	cl.client.Transport = t
}

// Get wraps a standard http.Get method which issues a GET to the specified URL.
func (cl *Client) Get(url string) (*http.Response, error) {
	return cl.client.Get(url)
}

// Do wraps a standard http.Do method which sends an HTTP request and returns an HTTP response.
func (cl *Client) Do(req *http.Request) (*http.Response, error) {
	return cl.client.Do(req)
}
