package packaging

import (
	"fmt"
	"github.com/barcodepro/pgscv/internal/log"
	"io"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"text/template"
)

const confFileTemplate = `{
  "api_key": "{{ .APIKey }}",
  "project_id": "{{ .ProjectID }}",
  "metrics_service_url": "{{ .MetricServiceBaseURL }}",
  "defaults": {
    "postgres_username": "pgscv",
    "postgres_password": "{{ .DefaultPostgresPassword }}",
    "pgbouncer_username": "pgscv",
    "pgbouncer_password": "{{ .DefaultPgbouncerPassword }}"
  }
}
`

const unitTemplate = `
[Unit]
Description=pgSCV is the Weaponry platform agent for PostgreSQL ecosystem
After=network.target

[Service]
Type=simple

User={{ .RunAsUser }}
Group={{ .RunAsUser }}

# Start the agent process
ExecStart=/usr/bin/{{ .AgentBinaryName }} --config-file=/etc/pgscv.json

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
	AgentBinaryName          string
	AutoStart                bool
	RunAsUser                string
	MetricServiceBaseURL     string
	APIKey                   string
	ProjectID                string
	DefaultPostgresPassword  string
	DefaultPgbouncerPassword string
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

	if c.MetricServiceBaseURL == "" {
		return fmt.Errorf("PGSCV_METRICS_SERVICE_BASE_URL is not defined")
	}
	if c.APIKey == "" {
		return fmt.Errorf("PGSCV_API_KEY is not defined")
	}
	if c.ProjectID == "" {
		return fmt.Errorf("PGSCV_PROJECT_ID is not defined")
	}

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

	if err := installBin(config); err != nil {
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
		if err := enableAutostart(config); err != nil {
			return bootstrapFailed(err)
		}
	}

	if err := runAgent(config); err != nil {
		return bootstrapFailed(err)
	}

	if err := deleteSelf(config); err != nil {
		return bootstrapFailed(err)
	}

	return bootstrapSuccessful()
}

// installs agent binary
func installBin(config *BootstrapConfig) error {
	log.Info("Install agent")
	fromFilename := fmt.Sprintf("./%s", config.AgentBinaryName)
	toFilename := fmt.Sprintf("/usr/bin/%s", config.AgentBinaryName)

	from, err := os.Open(fromFilename)
	if err != nil {
		return fmt.Errorf("open file failed: %s", err)

	}
	to, err := os.OpenFile(toFilename, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0755)
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
	conffile := fmt.Sprintf("/etc/%s.json", config.AgentBinaryName)
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

	unitfile := fmt.Sprintf("/etc/systemd/system/%s.service", config.AgentBinaryName)
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
func enableAutostart(config *BootstrapConfig) error {
	log.Info("Enable autostart")

	servicename := fmt.Sprintf("%s.service", config.AgentBinaryName)
	cmd := exec.Command("systemctl", "enable", servicename)
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
func runAgent(config *BootstrapConfig) error {
	log.Info("Run agent")

	servicename := fmt.Sprintf("%s.service", config.AgentBinaryName)
	cmd := exec.Command("systemctl", "start", servicename)
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
func deleteSelf(config *BootstrapConfig) error {
	log.Info("Cleanup")
	filename := fmt.Sprintf("./%s", config.AgentBinaryName)
	return os.Remove(filename)
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
