package packaging

import (
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

func Test_newGithubAPI(t *testing.T) {
	api := newGithubAPI("https://example.org")
	assert.NotNil(t, api)
}

func Test_githubAPI_request(t *testing.T) {
	testcases := []struct {
		valid           bool
		apiResponseCode int
		apiResponse     interface{}
		want            string
	}{
		{valid: true, apiResponseCode: http.StatusOK, apiResponse: "OK", want: "v0.0.1"},
		{valid: false, apiResponseCode: http.StatusNotFound},
	}

	for _, tc := range testcases {
		s := mockHTTPServer(t, tc.apiResponseCode, tc.apiResponse)
		api := newGithubAPI(s.URL)

		got, err := api.request("/")
		if tc.valid {
			assert.NoError(t, err)
			assert.NotNil(t, got)
		} else {
			assert.Error(t, err)
			assert.Nil(t, got)
		}

		s.Close()
	}

	// Test unreachable http server
	api := newGithubAPI("http://127.0.0.1:1234")
	got, err := api.request("/")
	assert.Error(t, err)
	assert.Nil(t, got)
}

func Test_githubAPI_getLatestRelease(t *testing.T) {
	testcases := []struct {
		valid           bool
		apiResponseCode int
		apiResponse     interface{}
		want            string
	}{
		{valid: true, apiResponseCode: http.StatusOK, apiResponse: map[string]string{"tag_name": "v0.0.1"}, want: "v0.0.1"},
		{valid: false, apiResponseCode: http.StatusNotFound},
		{valid: false, apiResponseCode: http.StatusOK, apiResponse: "invalid"},
		{valid: false, apiResponseCode: http.StatusOK, apiResponse: map[string]string{"test": "value"}},
	}

	for _, tc := range testcases {
		s := mockHTTPServer(t, tc.apiResponseCode, tc.apiResponse)
		api := newGithubAPI(s.URL)

		got, err := api.getLatestRelease()
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		} else {
			assert.Error(t, err)
		}

		s.Close()
	}

	// Test unreachable http server
	api := newGithubAPI("http://127.0.0.1:1234")
	_, err := api.getLatestRelease()
	assert.Error(t, err)
}

func Test_githubAPI_getLatestReleaseDownloadURL(t *testing.T) {
	testcases := []struct {
		valid           bool
		apiResponseCode int
		apiResponse     interface{}
		want            string
	}{
		{
			valid: true, apiResponseCode: http.StatusOK,
			apiResponse: map[string]interface{}{
				"assets": []interface{}{
					map[string]interface{}{"browser_download_url": "asset.zip"},
					map[string]interface{}{"browser_download_url": "asset.tar.gz"},
				},
			}, want: "asset.tar.gz",
		},
		{valid: false, apiResponseCode: http.StatusNotFound},
		{valid: false, apiResponseCode: http.StatusOK, apiResponse: "invalid"},
		{valid: false, apiResponseCode: http.StatusOK, apiResponse: map[string]string{"test": "value"}},
	}

	for _, tc := range testcases {
		s := mockHTTPServer(t, tc.apiResponseCode, tc.apiResponse)
		api := newGithubAPI(s.URL)

		got, err := api.getLatestReleaseDownloadURL("v0.0.1")
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		} else {
			assert.Error(t, err)
		}

		s.Close()
	}

	//Test unreachable http server
	api := newGithubAPI("http://127.0.0.1:1234")
	_, err := api.getLatestReleaseDownloadURL("v0.0.1")
	assert.Error(t, err)
}
