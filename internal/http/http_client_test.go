package http

import (
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

func TestClient_EnableTLSInsecure(t *testing.T) {
	cl := NewClient(ClientConfig{})

	assert.Nil(t, cl.client.Transport.(*http.Transport).TLSClientConfig)
	cl.EnableTLSInsecure()
	assert.True(t, cl.client.Transport.(*http.Transport).TLSClientConfig.InsecureSkipVerify)
}

func TestClient_Get(t *testing.T) {
	ts := TestServer(t, StatusOK, "")
	defer ts.Close()

	cl := NewClient(ClientConfig{})
	resp, err := cl.Get(ts.URL)
	assert.NoError(t, err)
	assert.NotNil(t, resp)

	_, err = cl.Get("http://[[")
	assert.Error(t, err)
}

func TestClient_Do(t *testing.T) {
	ts := TestServer(t, StatusOK, "")
	defer ts.Close()

	req, err := http.NewRequest("GET", ts.URL, nil)
	assert.NoError(t, err)

	cl := NewClient(ClientConfig{})

	resp, err := cl.Do(req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}
