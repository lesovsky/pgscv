package autoupdate

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

func mockHTTPServer(t *testing.T, code int, response interface{}) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if code == http.StatusOK {
			buf, err := json.Marshal(&response)
			assert.NoError(t, err)

			_, err = rw.Write(buf)
			assert.NoError(t, err)
		} else {
			rw.WriteHeader(code)
		}
	}))
}
