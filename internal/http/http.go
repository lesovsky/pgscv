package http

import (
	"crypto/tls"
	"net/http"
	"time"
)

const (
	StatusOK = http.StatusOK
)

type Client struct {
	client *http.Client
	Config ClientConfig
}

type ClientConfig struct {
	Timeout time.Duration
}

func NewClient(cfg ClientConfig) *Client {
	const defaultTimeout = time.Second

	if cfg.Timeout == 0 {
		cfg.Timeout = defaultTimeout
	}

	return &Client{
		client: &http.Client{Timeout: cfg.Timeout},
		Config: cfg,
	}
}

func (cl *Client) EnableTLSInsecure() {
	if cl.client.Transport != nil {
		return
	}

	cl.client.Transport = &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} // #nosec G402
}

func (cl *Client) Get(url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	return cl.client.Do(req)
}
