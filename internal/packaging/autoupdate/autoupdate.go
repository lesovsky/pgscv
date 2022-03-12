package autoupdate

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/lesovsky/pgscv/internal/http"
	"github.com/lesovsky/pgscv/internal/log"
	"golang.org/x/sys/unix"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Config define configuration for pgSCV auto-update procedure.
type Config struct {
	BinaryPath    string
	BinaryVersion string
	UpdatePolicy  string // controls auto-update source
}

// StartAutoupdateLoop is the background process which updates agent periodically
func StartAutoupdateLoop(ctx context.Context, c *Config) {
	// Check directory with program executable is writable.
	if err := checkExecutablePath(c.BinaryPath); err != nil {
		log.Errorf("auto-update cannot start: %s", err)
		return
	}

	rand.Seed(time.Now().UnixNano())
	itv := time.Duration(60+rand.Intn(60)) * time.Minute // #nosec G404

	log.Info("start background auto-update loop")
	for {
		err := runUpdate(c)
		if err != nil {
			log.Errorln("auto-update failed: ", err)
		}

		select {
		case <-time.After(itv):
			continue
		case <-ctx.Done():
			log.Info("exit signaled, stop auto-update loop")
			return
		}
	}
}

// runUpdate defines the whole step-by-step procedure for updating agent.
func runUpdate(c *Config) error {
	log.Debug("run update")

	api := newGithubAPI("https://api.github.com/repos")

	// Check the version of agent located by the URL.
	distVersion, err := api.getLatestRelease()
	if err != nil {
		return fmt.Errorf("check version failed: %s", err)
	}

	// Compare versions, if versions are the same - skip update.
	if distVersion == c.BinaryVersion {
		log.Debug("same version, update is not required, try next time")
		return nil
	}

	// If new version is release candidate, check the update policy - is -rc are allowed?
	if strings.Contains(distVersion, "-rc") && c.UpdatePolicy != "devel" {
		log.Debug("update to release candidate is forbidden, skip")
		return nil
	}

	log.Infof("starting auto-update from '%s' to '%s'", c.BinaryVersion, distVersion)

	// If versions different, get assets download URLs and download assets.
	downloadURL, checksumURL, err := api.getLatestReleaseDownloadURL(distVersion)
	if err != nil {
		return fmt.Errorf("request download urls failed: %s", err)
	}

	workDir := "/tmp/pgscv_" + distVersion
	err = os.Mkdir(workDir, 0750)
	if err != nil {
		return err
	}

	// Do cleanup in the end (in case of further error).
	defer doCleanup(workDir)

	// Download distribution and checksums file and store it in temporary directory.
	distFilePath, csumFilePath, err := downloadDistribution(downloadURL, checksumURL, workDir)
	if err != nil {
		return fmt.Errorf("download failed: %s", err)
	}

	// Checks SHA256 sums of downloaded dist with included SHA256-sum.
	err = checkDistributionChecksum(distFilePath, csumFilePath)
	if err != nil {
		return fmt.Errorf("compare checksum failed: %s; cancel update, try next time", err)
	}

	// Unpack distribution.
	extractedPath, err := extractDistribution(distFilePath, workDir)
	if err != nil {
		return fmt.Errorf("unpack archive failed: %s", err)
	}

	sourceFilePath := extractedPath + "/pgscv"

	// Update agent executable and restart the service.
	err = updateBinary(sourceFilePath, c.BinaryPath)
	if err != nil {
		return fmt.Errorf("update binary failed: %s", err)
	}

	// Explicit cleanup, because after restart execution of the code will interrupted.
	doCleanup(workDir)

	log.Infof("auto-update executable from '%s' to '%s' has been successful", c.BinaryVersion, distVersion)

	// Restart the service.
	err = restartSystemdService()
	if err != nil {
		return fmt.Errorf("update successful, but restarting systemd service has been failed: %s", err)
	}

	return nil
}

// githubAPI defines HTTP communication channel with Github API.
type githubAPI struct {
	baseURL string
	client  *http.Client
}

// newGithubAPI creates new Github API communication instance.
func newGithubAPI(baseURL string) *githubAPI {
	return &githubAPI{
		baseURL: baseURL,
		client:  http.NewClient(http.ClientConfig{Timeout: 10 * time.Second}),
	}
}

// request requests passed URL and returns raw response if request was successful.
func (api *githubAPI) request(url string) ([]byte, error) {
	response, err := api.client.Get(api.baseURL + url)
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
	buf, err := api.request("/lesovsky/pgscv/releases/latest")
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
func (api *githubAPI) getLatestReleaseDownloadURL(tag string) (string, string, error) {
	url := fmt.Sprintf("/lesovsky/pgscv/releases/tags/%s", tag)

	buf, err := api.request(url)
	if err != nil {
		return "", "", err
	}

	var data map[string]interface{}
	err = json.Unmarshal(buf, &data)
	if err != nil {
		return "", "", err
	}

	// Response should have array of assets.
	if _, ok := data["assets"]; !ok {
		return "", "", fmt.Errorf("assets not found in response")
	}

	assets := data["assets"].([]interface{})
	var downloadURL, checksumsURL string

	// Looking the 'browser_download_url' property which point to .tar.gz asset.
	for _, asset := range assets {
		if props, ok := asset.(map[string]interface{}); ok {
			if assetURL, propsOK := props["browser_download_url"].(string); propsOK {
				if strings.HasSuffix(assetURL, ".tar.gz") {
					downloadURL = assetURL
					continue
				}
				if strings.HasSuffix(assetURL, "checksums.txt") {
					checksumsURL = assetURL
					continue
				}
			}
		}
	}

	if downloadURL == "" || checksumsURL == "" {
		return "", "", fmt.Errorf("required assets not found in response: '%s','%s'", downloadURL, checksumsURL)
	}

	return downloadURL, checksumsURL, nil
}

// downloadDistribution downloads agent distribution, saves to destination dir and returns paths to saved files.
func downloadDistribution(distURL, csumURL, destDir string) (string, string, error) {
	log.Debug("download an agent distribution")

	distParts := strings.Split(distURL, "/")
	distFile := destDir + "/" + distParts[len(distParts)-1]

	csumParts := strings.Split(csumURL, "/")
	csumFile := destDir + "/" + csumParts[len(csumParts)-1]

	err := downloadFile(distURL, distFile)
	if err != nil {
		return "", "", err
	}
	err = downloadFile(csumURL, csumFile)
	if err != nil {
		return "", "", err
	}
	return distFile, csumFile, nil
}

// checkDistributionChecksum calculates checksum of file using checksum file.
func checkDistributionChecksum(distFilePath string, csumFilePath string) error {
	log.Debug("check agent distribution checksum")

	// Extract the filename from path.
	parts := strings.Split(distFilePath, "/")
	filename := parts[len(parts)-1]

	// Calculate the SHA256 hash of dist file.
	csumGot, err := hashSha256(filepath.Clean(distFilePath))
	if err != nil {
		return err
	}

	// Read checksums file and looking for checksum of dist file.
	f, err := os.Open(filepath.Clean(csumFilePath))
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	var csumWant string

	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 {
			return fmt.Errorf("checksum file invalid input")
		}

		if fields[1] == filename {
			csumWant = fields[0]
			break
		}
	}

	// Compare calculated checksum with checksum written in release file.
	if csumGot != csumWant {
		return fmt.Errorf("checksums are different, want: %s, got %s", csumWant, csumGot)
	}

	log.Debug("checksums ok")
	return nil
}

// extractDistribution extracts files from archive to specified destination directory. Returns directory path of
// extracted files.
func extractDistribution(distFilePath string, destDir string) (string, error) {
	log.Debug("extracting agent distribution")

	r, err := os.Open(filepath.Clean(distFilePath))
	if err != nil {
		return "", err
	}

	uncompressedStream, err := gzip.NewReader(r)
	if err != nil {
		return "", err
	}

	parts := strings.Split(distFilePath, "/")
	dirname := destDir + "/" + strings.TrimSuffix(parts[len(parts)-1], ".tar.gz")

	err = os.Mkdir(dirname, 0750)
	if err != nil {
		return "", err
	}

	tarReader := tar.NewReader(uncompressedStream)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		switch header.Typeflag {
		case tar.TypeReg:
			outFile, err := os.Create(dirname + "/" + header.Name) // #nosec G304
			if err != nil {
				return "", err
			}

			// TODO: G110 suppressed because it's not clear how to fix it.
			_, err = io.Copy(outFile, tarReader) // #nosec G110
			if err != nil {
				return "", err
			}

			err = outFile.Close()
			if err != nil {
				log.Warnf("close file failed: %s; ignore", err)
			}
		default:
			return "", fmt.Errorf("unknown file type: %d in %s", header.Typeflag, header.Name)
		}
	}
	log.Debug("extract finished")
	return dirname, nil
}

// updateBinary replaces existing agent executable with new version.
func updateBinary(sourceFile string, destFile string) error {
	log.Debug("start agent binary update")

	if sourceFile == "" || destFile == "" {
		return fmt.Errorf("invalid input: source '%s', destination '%s'", sourceFile, destFile)
	}

	in, err := os.ReadFile(sourceFile) // #nosec G304
	if err != nil {
		return fmt.Errorf("read source file failed: %s", err)
	}

	// remove old binary
	err = os.Remove(destFile)
	if err != nil {
		return err
	}

	err = os.WriteFile(destFile, in, 0600)
	if err != nil {
		return fmt.Errorf("write destination file failed: %s", err)
	}

	err = os.Chmod(destFile, 0755) // #nosec G302
	if err != nil {
		return fmt.Errorf("chmod 0755 failed: %s", err)
	}

	return nil
}

// restartSystemdService checks privileges and restarts pgscv service.
func restartSystemdService() error {
	var name string
	var args []string

	if os.Geteuid() == 0 {
		name, args = "systemctl", []string{"restart", "pgscv.service"}
	} else {
		name, args = "sudo", []string{"systemctl", "restart", "pgscv.service"}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := exec.CommandContext(ctx, name, args...).Run() // #nosec G204
	if err != nil {
		return fmt.Errorf("restarting service failed: %s", err)
	}

	return nil
}

// doCleanup removes agent distribution files from tmp directory
func doCleanup(path string) {
	if path == "" || path == "/" {
		log.Warnf("invalid input, bad path: '%s', skip", path)
		return
	}

	err := os.RemoveAll(path)
	if err != nil {
		log.Warnf("removing '%s' failed: %s; ignore it; ", path, err)
	}
}

// downloadFile downloads file using passed URL.
func downloadFile(url, file string) error {
	log.Debugf("download using %s to %s", url, file)

	if url == "" || file == "" {
		return fmt.Errorf("invalid input: url '%s', file '%s'", url, file)
	}

	client := http.NewClient(http.ClientConfig{Timeout: 10 * time.Second})
	resp, err := client.Get(url)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download failed, %d", resp.StatusCode)
	}

	out, err := os.Create(file) // #nosec G304
	if err != nil {
		return err
	}
	defer func() {
		err = out.Close()
		if err != nil {
			log.Warnf("close file failed: %s; ignore", err)
		}
	}()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	_ = resp.Body.Close()

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
		err = f.Close()
		if err != nil {
			log.Warnf("close file failed: %s; ignore", err)
		}
	}()

	h := sha256.New()
	_, err = io.Copy(h, f)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// checkExecutablePath checks path of an executable and it is suitable for update procedure (has write permissions).
func checkExecutablePath(path string) error {
	if !strings.HasPrefix(path, "/") {
		return fmt.Errorf("relative path specified: '%s'", path)
	}

	fields := strings.Split(path, "/")
	if len(fields) <= 2 {
		return fmt.Errorf("invalid  input '%s'", path)
	}

	rundir := strings.Join(fields[0:len(fields)-1], "/")
	if rundir == "" {
		rundir = "/"
	}

	return unix.Access(rundir, unix.W_OK)
}
