package packaging

import (
	"fmt"
	"github.com/barcodepro/pgscv/internal/log"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"text/template"
)

const confFileTemplate = `autoupdate_url: "{{ .AutoUpdateURL}}"
api_key: "{{ .APIKey }}"
project_id: {{ .ProjectID }}
send_metrics_url: "{{ .SendMetricsURL }}"
defaults:
    postgres_username: "pgscv"
    postgres_password: "{{ .DefaultPostgresPassword }}"
    pgbouncer_username: "pgscv"
    pgbouncer_password: "{{ .DefaultPgbouncerPassword }}"
`

const unitTemplate = `
[Unit]
Description=pgSCV is the Weaponry platform agent for PostgreSQL ecosystem
Requires=network-online.target
After=network-online.target

[Service]
Type=simple

User={{ .RunAsUser }}
Group={{ .RunAsUser }}

# Start the agent process
ExecStart=/usr/bin/{{ .ExecutableName }} --config-file=/etc/{{ .ExecutableName }}.yaml

# Only kill the agent process
KillMode=process

# Wait reasonable amount of time for agent up/down
TimeoutSec=5

# Restart agent if it crashes
Restart=on-failure

# if agent leaks during long period of time, let him to be the first person for eviction
OOMScoreAdjust=1000

[Install]
WantedBy=multi-user.target
`

type BootstrapConfig struct {
	ExecutableName           string
	AutoStart                bool
	RunAsUser                string
	SendMetricsURL           string
	AutoUpdateURL            string
	APIKey                   string
	ProjectID                string
	DefaultPostgresPassword  string
	DefaultPgbouncerPassword string
	//
	configPathPrefix  string // path prefix for configuration file
	systemdPathPrefix string // path prefix for systemd units
}

func (c *BootstrapConfig) Validate() error {
	log.Infoln("Validate bootstrap configuration")

	if c.RunAsUser == "" {
		c.RunAsUser = "root"
	}

	_, err := user.Lookup(c.RunAsUser)
	if err != nil {
		return fmt.Errorf("specified user does not exists: %s ", err)
	}

	if c.SendMetricsURL == "" {
		return fmt.Errorf("PGSCV_SEND_METRICS_URL is not defined")
	}
	if c.AutoUpdateURL == "" {
		return fmt.Errorf("PGSCV_AUTOUPDATE_URL is not defined")
	}
	if c.APIKey == "" {
		return fmt.Errorf("PGSCV_API_KEY is not defined")
	}
	if c.ProjectID == "" {
		return fmt.Errorf("PGSCV_PROJECT_ID is not defined")
	}

	c.ExecutableName = defaultExecutableName
	c.configPathPrefix = defaultConfigPathPrefix
	c.systemdPathPrefix = defaultSystemdPathPrefix

	return nil
}

// RunBootstrap is the main bootstrap entry point
func RunBootstrap(config *BootstrapConfig) int {
	log.Info("Running bootstrap")
	if err := preCheck(); err != nil {
		return bootstrapFailed(err)
	}

	if err := config.Validate(); err != nil {
		return bootstrapFailed(err)
	}

	if err := installBin(); err != nil {
		return bootstrapFailed(err)
	}

	if err := createConfigFile(config); err != nil {
		return bootstrapFailed(err)
	}

	if err := createSystemdUnit(config); err != nil {
		return bootstrapFailed(err)
	}

	if err := reloadSystemd(); err != nil {
		return bootstrapFailed(err)
	}

	if config.AutoStart {
		if err := enableAutostart(); err != nil {
			return bootstrapFailed(err)
		}
	}

	if err := runAgent(); err != nil {
		return bootstrapFailed(err)
	}

	if err := deleteSelf(); err != nil {
		return bootstrapFailed(err)
	}

	return bootstrapSuccessful()
}

// installs agent binary
func installBin() error {
	log.Info("Install agent")
	fromFilename := fmt.Sprintf("./%s", defaultExecutableName)
	toFilename := fmt.Sprintf("/usr/bin/%s", defaultExecutableName)

	from, err := os.Open(filepath.Clean(fromFilename))
	if err != nil {
		return fmt.Errorf("open file failed: %s", err)

	}
	to, err := os.OpenFile(filepath.Clean(toFilename), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
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
	return nil
}

// createConfigFile creates config file
func createConfigFile(config *BootstrapConfig) error {
	log.Info("Create config file")

	uid, gid, err := getUserIDs(config.RunAsUser)
	if err != nil {
		return fmt.Errorf("failed get user's uid/gid: %s", err)
	}

	// create config-file with proper permissions
	conffile := fmt.Sprintf("%s/%s.yaml", config.configPathPrefix, config.ExecutableName)
	f, err := os.Create(conffile)
	if err != nil {
		return fmt.Errorf("create config file failed: %s", err)
	}

	if err = f.Chown(uid, gid); err != nil {
		return fmt.Errorf("change file ownership failed: %s", err)
	}

	if err = f.Chmod(0600); err != nil {
		return fmt.Errorf("change file permissions failed: %s", err)
	}

	// write content using template
	t, err := template.New("conf").Parse(confFileTemplate)
	if err != nil {
		return fmt.Errorf("parse template failed: %s", err)
	}
	err = t.Execute(f, config)
	if err != nil {
		return fmt.Errorf("execute template failed: %s", err)
	}

	if err = f.Close(); err != nil {
		log.Warnln("close file failed, ignore it; ", err)
	}

	return nil
}

// creates systemd unit in system path
func createSystemdUnit(config *BootstrapConfig) error {
	log.Info("Create systemd unit")
	t, err := template.New("unit").Parse(unitTemplate)
	if err != nil {
		return fmt.Errorf("parse template failed: %s", err)
	}

	unitfile := fmt.Sprintf("%s/%s", config.systemdPathPrefix, systemdServiceName)
	f, err := os.Create(unitfile)
	if err != nil {
		return fmt.Errorf("create file failed: %s ", err)
	}

	err = t.Execute(f, config)
	if err != nil {
		return fmt.Errorf("execute template failed: %s ", err)
	}

	if err = f.Close(); err != nil {
		log.Warnln("close file failed, ignore it; ", err)
	}
	return nil
}

// reloads systemd
func reloadSystemd() error {
	log.Info("Reload systemd")
	cmd := exec.Command("systemctl", "daemon-reload")
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("systemd reload failed: %s ", err)
	}

	log.Info("bootstrap: waiting until systemd daemon-reload to finish...")
	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("systemd reload failed: %s ", err)
	}
	return nil
}

// enables agent autostart
func enableAutostart() error {
	log.Info("Enable autostart")

	cmd := exec.Command("systemctl", "enable", systemdServiceName)
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("enable agent service failed: %s ", err)
	}
	log.Info("bootstrap: waiting until systemd enables agent service...")

	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("systemd enable service failed: %s ", err)
	}
	return nil
}

// run agent systemd unit
func runAgent() error {
	log.Info("Run agent")

	cmd := exec.Command("systemctl", "start", systemdServiceName)
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("start agent service failed: %s ", err)

	}
	log.Info("bootstrap: waiting until systemd starts agent service...")

	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("systemd starting service failed: %s ", err)
	}
	return nil
}

// delete self executable
func deleteSelf() error {
	log.Info("Cleanup")
	return os.Remove(filepath.Clean(fmt.Sprintf("./%s", defaultExecutableName)))
}

// bootstrapFailed signales bootstrap failed with error
func bootstrapFailed(e error) int {
	log.Errorln("Stop bootstrap: ", e)
	return 1
}

// bootstrapSuccessful signales bootstrap finished successfully
func bootstrapSuccessful() int {
	log.Info("Bootstrap successful")
	return 0
}

// getUserIDs returns numeric UID and GID of passed user
func getUserIDs(username string) (int, int, error) {
	u, err := user.Lookup(username)
	if err != nil {
		return -1, -1, err
	}

	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		return -1, -1, err
	}

	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		return -1, -1, err
	}

	return uid, gid, nil
}
