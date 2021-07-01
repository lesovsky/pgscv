package autoupdate

import (
	"github.com/stretchr/testify/assert"
	"github.com/weaponry/pgscv/internal/http"
	"os"
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
		apiResponse     string
		want            string
	}{
		{valid: true, apiResponseCode: http.StatusOK, apiResponse: "OK", want: "v0.0.1"},
		{valid: false, apiResponseCode: http.StatusNotFound},
	}

	for _, tc := range testcases {
		s := http.TestServer(t, tc.apiResponseCode, tc.apiResponse)
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
		apiResponse     string
		want            string
	}{
		{valid: true, apiResponseCode: http.StatusOK, apiResponse: `{"tag_name": "v0.0.1"}`, want: "v0.0.1"},
		{valid: false, apiResponseCode: http.StatusNotFound},
		{valid: false, apiResponseCode: http.StatusOK, apiResponse: "invalid"},
		{valid: false, apiResponseCode: http.StatusOK, apiResponse: `{"test": "value"}`},
	}

	for _, tc := range testcases {
		s := http.TestServer(t, tc.apiResponseCode, tc.apiResponse)
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
		apiResponse     string
		wantDist        string
		wantCsum        string
	}{
		{
			valid:           true,
			apiResponseCode: http.StatusOK,
			apiResponse:     `{"assets":[{"browser_download_url":"checksums.txt"},{"browser_download_url":"asset.zip"},{"browser_download_url":"asset.tar.gz"}]}`,
			wantDist:        "asset.tar.gz", wantCsum: "checksums.txt",
		},
		{valid: false, apiResponseCode: http.StatusNotFound},
		{valid: false, apiResponseCode: http.StatusOK, apiResponse: "invalid"},
		{valid: false, apiResponseCode: http.StatusOK, apiResponse: `{"test": "value"}`},
	}

	for _, tc := range testcases {
		s := http.TestServer(t, tc.apiResponseCode, tc.apiResponse)
		api := newGithubAPI(s.URL)

		dist, csum, err := api.getLatestReleaseDownloadURL("v0.0.1")
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.wantDist, dist)
			assert.Equal(t, tc.wantCsum, csum)
		} else {
			assert.Error(t, err)
		}

		s.Close()
	}

	// Test unreachable http server
	api := newGithubAPI("http://127.0.0.1:1234")
	_, _, err := api.getLatestReleaseDownloadURL("v0.0.1")
	assert.Error(t, err)
}

func Test_downloadDistribution(t *testing.T) {
	s := http.TestFileServer(t, "./testdata")

	testcases := []struct {
		valid    bool
		dpath    string
		spath    string
		wantDist string
		wantSum  string
	}{
		{valid: true, dpath: s.URL + "/example.golden.tar.gz", spath: s.URL + "/checksums.golden.txt", wantDist: "/tmp/example.golden.tar.gz", wantSum: "/tmp/checksums.golden.txt"},
		{valid: false, dpath: "", spath: s.URL + "/checksums.golden.txt"},
		{valid: false, dpath: s.URL + "/example.golden.tar.gz", spath: ""},
		{valid: false, dpath: "", spath: ""},
	}

	for _, tc := range testcases {
		if tc.valid {
			gotDist, gotSum, err := downloadDistribution(tc.dpath, tc.spath, "/tmp")
			assert.NoError(t, err)
			assert.Equal(t, tc.wantDist, gotDist)
			assert.Equal(t, tc.wantSum, gotSum)
			assert.NoError(t, os.Remove(gotDist))
			assert.NoError(t, os.Remove(gotSum))
		} else {
			_, _, err := downloadDistribution(tc.dpath, tc.spath, "/tmp")
			assert.Error(t, err)
		}
	}

	s.Close()
}

func Test_checkDistributionChecksum(t *testing.T) {
	testcases := []struct {
		valid   bool
		srcfile string
		sumfile string
	}{
		{valid: true, srcfile: "./testdata/example.golden.tar.gz", sumfile: "./testdata/checksums.golden.txt"},
		{valid: false, srcfile: "", sumfile: "./testdata/checksums.golden.txt"},
		{valid: false, srcfile: "./testdata/example.golden.tar.gz", sumfile: ""},
		{valid: false, srcfile: "./testdata/example.golden.tar.gz", sumfile: "./testdata/checksums.NOTFOUND.txt"},
		{valid: false, srcfile: "./testdata/example.golden.tar.gz", sumfile: "./testdata/checksums.invalid.txt"},
		{valid: false, srcfile: "./testdata/example.golden.tar.gz", sumfile: "./testdata/checksums.invalid.2.txt"},
	}

	for _, tc := range testcases {
		if tc.valid {
			assert.NoError(t, checkDistributionChecksum(tc.srcfile, tc.sumfile))
		} else {
			assert.Error(t, checkDistributionChecksum(tc.srcfile, tc.sumfile))
		}
	}
}

func Test_extractDistribution(t *testing.T) {
	testcases := []struct {
		valid   bool
		srcfile string
		want    string
	}{
		{valid: true, srcfile: "./testdata/example.golden.tar.gz", want: "/tmp/example.golden"},
		{valid: false, srcfile: "./testdata/example.NOTFOUND.tar.gz"},
		{valid: false, srcfile: "./testdata/example.invalid.tar.gz"},
		{valid: false, srcfile: "./testdata/example.invalid"},
		{valid: false, srcfile: ""},
	}

	for _, tc := range testcases {
		if tc.valid {
			_, err := os.Stat("/tmp/example.txt")
			assert.Error(t, err)

			got, err := extractDistribution(tc.srcfile, "/tmp")
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)

			_, err = os.Stat(got)
			assert.NoError(t, err)

			assert.NoError(t, os.RemoveAll(got))
		} else {
			_, err := extractDistribution(tc.srcfile, "/tmp")
			assert.Error(t, err)
		}
	}
}

func Test_updateBinary(t *testing.T) {
	testcases := []struct {
		valid bool
		from  string
		to    string
	}{
		{valid: true, from: "./testdata/example.golden.tar.gz", to: "/tmp/test-copy.golden.tar.gz"},
		{valid: false, from: "./testdata/example.UNKNOWN.tar.gz", to: "/tmp/test-copy.golden.tar.gz"},
		{valid: false, from: "./testdata/example.golden.tar.gz", to: "/tmp/test-copy.UNKNOWN.tar.gz"},
		{valid: false, from: "", to: "/tmp/test-copy.UNKNOWN.tar.gz"},
		{valid: false, from: "./testdata/example.golden.tar.gz", to: ""},
	}

	for _, tc := range testcases {
		if tc.valid {
			// create temporary empty file
			f, err := os.Create(tc.to)
			assert.NoError(t, err)
			assert.NoError(t, f.Close())

			assert.NoError(t, updateBinary(tc.from, tc.to))

			// compare sizes
			st1, err := os.Stat(tc.from)
			assert.NoError(t, err)
			st2, err := os.Stat(tc.to)
			assert.NoError(t, err)

			assert.Equal(t, st1.Size(), st2.Size())

			// cleanup
			assert.NoError(t, os.Remove(tc.to))
		} else {
			assert.Error(t, updateBinary(tc.from, tc.to))
		}
	}

}

func Test_downloadFile(t *testing.T) {
	testcases := []struct {
		valid  bool
		path   string
		saveto string
	}{
		{valid: true, path: "/example.golden.tar.gz", saveto: "/tmp/example.golden.tar.gz"},
		{valid: false, path: "/example.golden.tar.gz", saveto: "/invalid/example.golden.tar.gz"},
		{valid: false, path: "/example.NOTFOUND.tar.gz", saveto: "/tmp/example.golden.tar.gz"},
		{valid: false, path: "/example.NOTFOUND.tar.gz", saveto: ""},
	}

	s := http.TestFileServer(t, "./testdata")

	for _, tc := range testcases {
		if tc.valid {
			assert.NoError(t, downloadFile(s.URL+tc.path, tc.saveto))
			assert.NoError(t, os.Remove(tc.saveto))
		} else {
			assert.Error(t, downloadFile(s.URL+tc.path, tc.saveto))
		}
	}

	s.Close()
}

func Test_hashSha256(t *testing.T) {
	testcases := []struct {
		valid   bool
		srcfile string
		want    string
	}{
		{valid: true, srcfile: "./testdata/example.golden.tar.gz", want: "39c808c828c35de7cfcc9b2edbcd2857445bb01dace4271d0e313de2a4e2449d"},
		{valid: false, srcfile: "./testdata/example.NOTFOUND.tar.gz"},
	}

	for _, tc := range testcases {
		got, err := hashSha256(tc.srcfile)
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		} else {
			assert.Error(t, err)
		}

	}
}

func Test_checkExecutablePath(t *testing.T) {
	testcases := []struct {
		valid bool
		path  string
	}{
		{valid: true, path: "/tmp/example"},
		{valid: true, path: "/tmp/"},
		{valid: false, path: "./example"},
		{valid: false, path: "example"},
		{valid: false, path: "/"},
		{valid: false, path: ""},
	}

	for _, tc := range testcases {
		err := checkExecutablePath(tc.path)
		if tc.valid {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}
