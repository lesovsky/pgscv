package packaging

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/weaponry/pgscv/internal/log"
	"golang.org/x/sys/unix"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type AutoupdateConfig struct {
	BinaryPath    string
	BinaryVersion string
	DistBaseURL   string
}

const (
	tmpDir           = "/tmp"
	fileBinary       = "pgscv"
	fileVersion      = "pgscv.version"
	fileSha256Sum    = "pgscv.sha256"
	fileDistribution = "pgscv.tar.gz"

	defaultAutoUpdateInterval = 5 * time.Minute
)

// githubAPI defines HTTP communication channel with Github API.
type githubAPI struct {
	baseURL string
	client  *http.Client
}

// newGithubAPI creates new Github API communication instance.
func newGithubAPI(baseURL string) *githubAPI {
	return &githubAPI{
		baseURL: baseURL,
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:    5,
				IdleConnTimeout: 60 * time.Second,
			},
			Timeout: 10 * time.Second,
		},
	}
}

// request requests passed URL and returns raw response if request was successful.
func (api *githubAPI) request(url string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, api.baseURL+url, nil)
	if err != nil {
		return nil, err
	}

	response, err := api.client.Do(req)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad HTTP response code: %d", response.StatusCode)
	}

	buf, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	err = response.Body.Close()
	if err != nil {
		log.Warnf("failed to close response body: %s; ignore it", err)
	}

	return buf, nil
}

// getLatestRelease returns string with pgSCV latest release on Github.
func (api *githubAPI) getLatestRelease() (string, error) {
	buf, err := api.request("/weaponry/pgscv/releases/latest")
	if err != nil {
		return "", err
	}

	var data map[string]interface{}
	err = json.Unmarshal(buf, &data)
	if err != nil {
		return "", err
	}

	// Looking for 'tag_name' property.
	if _, ok := data["tag_name"]; !ok {
		return "", fmt.Errorf("tag_name not found in response")
	}

	return data["tag_name"].(string), nil
}

// getLatestReleaseDownloadURL returns asset's download URL of the latest release.
func (api *githubAPI) getLatestReleaseDownloadURL(tag string) (string, error) {
	url := fmt.Sprintf("/weaponry/pgscv/releases/tags/%s", tag)

	buf, err := api.request(url)
	if err != nil {
		return "", err
	}

	var data map[string]interface{}
	err = json.Unmarshal(buf, &data)
	if err != nil {
		return "", err
	}

	// Response should have array of assets.
	if _, ok := data["assets"]; !ok {
		return "", fmt.Errorf("assets not found in response")
	}

	assets := data["assets"].([]interface{})
	var downloadURL string

	// Looking the 'browser_download_url' property which point to .tar.gz asset.
	for _, asset := range assets {
		if props, ok := asset.(map[string]interface{}); ok {
			if assetURL, propsOK := props["browser_download_url"].(string); propsOK {
				if strings.HasSuffix(assetURL, ".tar.gz") {
					downloadURL = assetURL
					break
				}
			}
		}
	}

	if downloadURL == "" {
		return "", fmt.Errorf(".tar.gz asset not found in response")
	}

	return downloadURL, nil
}

// StartBackgroundAutoUpdate is the background process which updates agent periodically
func StartBackgroundAutoUpdate(ctx context.Context, c *AutoupdateConfig) {
	// Check directory with program executable is writable.
	if err := checkRunDirectory(c.BinaryPath); err != nil {
		log.Errorf("auto-update cannot start: %s", err)
		return
	}

	log.Info("start background auto-update loop")
	for {
		err := RunUpdate(c)
		if err != nil {
			log.Errorln("auto-update failed: ", err)
		}

		select {
		case <-time.After(defaultAutoUpdateInterval):
			continue
		case <-ctx.Done():
			log.Info("exit signaled, stop auto-update")
			return
		}
	}
}

// RunUpdate is the main entry point for updating agent
func RunUpdate(c *AutoupdateConfig) error {
	log.Debug("run update")

	// check the version of agent located by the URL
	distVersion, err := getDistributionVersion(c.DistBaseURL)
	if err != nil {
		return fmt.Errorf("check version failure: %s", err)
	}

	// skip update procedure if the version is the same as the version of running agent
	if distVersion == c.BinaryVersion {
		log.Debug("same version, update is not required, exit")
		return nil
	}

	// versions are differ, download a new version of agent distribution
	err = downloadNewVersion(c.DistBaseURL)
	if err != nil {
		return fmt.Errorf("download failure: %s", err)
	}

	// checks SHA256 sums of downloaded dist with included SHA256-sum
	err = checkDistributionChecksum()
	if err != nil {
		doCleanup()
		return fmt.Errorf("checksum failure: %s", err)
	}

	// unpack distribution
	if err = extractDistribution(); err != nil {
		return fmt.Errorf("extract failure: %s", err)
	}

	// update agent and restart the service
	if err := updateBinary(); err != nil {
		return fmt.Errorf("update binary failure: %s", err)
	}

	// remove dist files
	doCleanup()

	log.Info("agent update successful")
	return nil
}

// getDistributionVersion returns version of remote distribution
func getDistributionVersion(baseURL string) (string, error) {
	log.Debug("getting a distribution version")

	var client = http.Client{Timeout: 10 * time.Second}
	var url = baseURL + "/" + fileVersion

	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("get failed, %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(bodyBytes)), nil
}

// downloadNewVersion downloads agent distribution and saves to tmpdir
func downloadNewVersion(baseURL string) error {
	log.Debug("download an agent distribution")

	var (
		distURL  = baseURL + "/" + fileDistribution
		distFile = tmpDir + "/" + fileDistribution
		sumURL   = baseURL + "/" + fileSha256Sum
		sumFile  = tmpDir + "/" + fileSha256Sum
	)

	err := downloadFile(distURL, distFile)
	if err != nil {
		return err
	}
	err = downloadFile(sumURL, sumFile)
	if err != nil {
		return err
	}
	return nil
}

// checkDistributionChecksum calculates checksum of the downloaded agent distribution and checks with pre-calculated
// checksum from distribution.
func checkDistributionChecksum() error {
	log.Debug("check agent distribution checksum")

	var distFilename = tmpDir + "/" + fileDistribution
	var distSumFilename = tmpDir + "/" + fileSha256Sum
	got, err := hashSha256(distFilename)
	if err != nil {
		return err
	}

	content, err := os.ReadFile(distSumFilename)
	if err != nil {
		return err
	}
	reader := bufio.NewReader(bytes.NewBuffer(content))
	line, _, err := reader.ReadLine()
	if err != nil {
		return err
	}
	fields := strings.Fields(string(line))
	if len(fields) != 2 {
		return fmt.Errorf("bad content")
	}
	var want string
	if fields[1] == fileDistribution {
		want = fields[0]
	}
	if got != want {
		return fmt.Errorf("checksums differs, want: %s, got %s", want, got)
	}
	log.Debug("checksums ok")
	return nil
}

// extractDistribution reads distribution and extracts agent's binary
func extractDistribution() error {
	log.Debug("extracting agent distribution")

	var distFile = tmpDir + "/" + fileDistribution
	r, err := os.Open(distFile)
	if err != nil {
		return err
	}

	uncompressedStream, err := gzip.NewReader(r)
	if err != nil {
		return err
	}

	tarReader := tar.NewReader(uncompressedStream)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.Mkdir(header.Name, 0750); err != nil {
				return err
			}
		case tar.TypeReg:
			outFile, err := os.Create(tmpDir + "/" + header.Name)
			if err != nil {
				return err
			}
			// TODO: warning excluded because it's not clear how to fix it.
			_, err = io.Copy(outFile, tarReader) // #nosec G110
			if err != nil {
				return err
			}
			err = outFile.Close()
			if err != nil {
				log.Warnf("something went wrong when closing file descriptor: %s; ignore it", err)
			}

		default:
			return fmt.Errorf("uknown type: %d in %s", header.Typeflag, header.Name)
		}
	}
	log.Debug("extract finished")
	return nil
}

// updateBinary replaces existing agent executable with new version
func updateBinary() error {
	log.Debug("start agent binary update")

	var fromFilename = tmpDir + "/" + fileBinary
	var toFilename = "/usr/bin/" + fileBinary

	// remove old binary
	if err := os.Remove(toFilename); err != nil {
		return err
	}

	// copy new to old
	from, err := os.Open(fromFilename)
	if err != nil {
		return fmt.Errorf("open file failed: %s", err)

	}
	to, err := os.OpenFile(toFilename, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("open destination file failed: %s", err)
	}
	_, err = io.Copy(to, from)
	if err != nil {
		return fmt.Errorf("copy file failed: %s", err)
	}
	if err = from.Close(); err != nil {
		log.Warnln("close source file failed, ignore it; ", err)
	}
	if err = to.Close(); err != nil {
		log.Warnln("close destination file failed, ignore it; ", err)
	}
	err = os.Chmod(toFilename, 0755) // #nosec G302
	if err != nil {
		return fmt.Errorf("chmod 0755 failed: %s", err)
	}

	// run service restart
	log.Debug("restarting the service")
	cmd := exec.Command("systemctl", "restart", systemdServiceName)
	// after cmd.Start execution of this code could be interrupted, end even err might not be handled.
	err = cmd.Start()
	if err != nil {
		return err
	}

	// should not be here, but who knows
	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("systemd starting service failed: %s ", err)
	}
	return nil
}

// doCleanup removes agent distribution files from tmp directory
func doCleanup() {
	log.Debug("cleanup agent distribution files")
	var (
		distFile = tmpDir + "/" + fileDistribution
		sumFile  = tmpDir + "/" + fileSha256Sum
		binFile  = tmpDir + "/" + fileBinary
	)

	for _, file := range []string{distFile, sumFile, binFile} {
		if err := os.Remove(file); err != nil {
			log.Warnln("failed remove file, ignore it; ", err)
		}
	}
}

// downloadFile downloads file and saves it locally
func downloadFile(url, file string) error {
	log.Debugf("download using %s to %s", url, file)
	var client = http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("get failed, %d", resp.StatusCode)
	}

	out, err := os.Create(file)
	if err != nil {
		return err
	}
	defer func() {
		if err := out.Close(); err != nil {
			log.Warnf("something went wrong when closing file descriptor: %s; ignore it", err)
		}
	}()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}
	return nil
}

// hashSha256 calculates sha256 for specified file
func hashSha256(filename string) (string, error) {
	log.Debugf("calculating sha256 checksum for %s", filename)
	f, err := os.Open(filepath.Clean(filename))
	if err != nil {
		return "", err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Warnf("something went wrong when closing file descriptor: %s; ignore it", err)
		}
	}()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// checkRunDirectory checks directory of passed path is writable.
func checkRunDirectory(path string) error {
	fields := strings.Split(path, "/")
	if len(fields) == 0 {
		return fmt.Errorf("empty slice")
	}

	rundir := strings.Join(fields[0:len(fields)-1], "/")
	return unix.Access(rundir, unix.W_OK)
}
