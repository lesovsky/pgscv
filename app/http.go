package app

import "net/http"

// кастомная реализация http клиента, с помощью которой мы будем добавлять допольнительные заголовки к запросам
type httpClient struct {
	client http.Client
	apiKey string
}

func NewHttpClient(key string) *httpClient {
	c := http.Client{}
	return &httpClient{client: c, apiKey: key}
}

// кастомная реализация метода для отправки запросов
func (c *httpClient) Do(req *http.Request) (*http.Response, error) {
	req.Header.Add("X-Weaponry-Api-Key", c.apiKey)
	return c.client.Do(req)
}
