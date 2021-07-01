package http

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServer(t *testing.T, code int, response string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if code == http.StatusOK {
			if response != "" {
				_, err := fmt.Fprintln(rw, response)
				assert.NoError(t, err)
			} else {
				rw.WriteHeader(code)
			}
		} else {
			rw.WriteHeader(code)
		}
	}))
}

func TestFileServer(_ *testing.T, dir string) *httptest.Server {
	return httptest.NewServer(http.FileServer(http.Dir(dir)))
}
