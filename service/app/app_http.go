package app

import "net/http"

// кастомная реализация http клиента, с помощью которой мы будем добавлять допольнительные заголовки к запросам
type httpClient struct {
	client http.Client
	apiKey string
}

// newHTTPClient ...
func newHTTPClient(key string) *httpClient {
	c := http.Client{}
	return &httpClient{client: c, apiKey: key}
}

// Do is the customizable way for sending HTTP requests
func (c *httpClient) Do(req *http.Request) (*http.Response, error) {
	req.Header.Add("X-Weaponry-Api-Key", c.apiKey)
	return c.client.Do(req)
}
