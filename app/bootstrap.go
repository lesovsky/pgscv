package app

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"io"
	"os"
	"os/exec"
	"text/template"
	"time"
)

const envFileTemplate = `API_KEY={{ .APIKey }}
METRIC_SERVICE_BASE_URL={{ .MetricServiceBaseURL }}
SEND_INTERVAL={{ .SendInterval }}
PG_PASSWORD={{ .Credentials.PostgresPass }}
PGB_PASSWORD={{ .Credentials.PgbouncerPass }}
`

const unitTemplate = `
[Unit]
Description={{ .AgentBinaryName }} is the Weaponry platform agent for PostgreSQL ecosystem
After=syslog.target network.target

[Service]
Type=simple

User=root
Group=root

EnvironmentFile=/etc/environment.d/weaponry-agent.conf
WorkingDirectory=~

# Start the agent process
ExecStart=/usr/local/bin/{{ .AgentBinaryName }}

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

type bootstrapConfig struct {
	AgentBinaryName      string
	MetricServiceBaseURL string        `json:"metric_service_base_url"`
	SendInterval         time.Duration `json:"send_interval"`
	APIKey               string        `json:"api_key"`
	AutoStart            bool          `json:"autostart"`
}

func newBootstrapConfig(appconfig *Config) *bootstrapConfig {
	return &bootstrapConfig{
		APIKey:               appconfig.APIKey,
		AgentBinaryName:      appconfig.BootstrapBinaryName,
		MetricServiceBaseURL: appconfig.MetricServiceBaseURL.String(),
		SendInterval:         appconfig.MetricsSendInterval,
	}
}

// RunBootstrap is the main bootstrap entry point
func RunBootstrap(appconfig *Config) int {
	log.Info().Msg("Running bootstrap")
	if err := preCheck(); err != nil {
		return bootstrapFailed(err)
	}

	config := newBootstrapConfig(appconfig)

	if err := installBin(config); err != nil {
		return bootstrapFailed(err)
	}

	if err := createEnvironmentFile(config); err != nil {
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

// run pre-bootstrap checks
func preCheck() error {
	log.Info().Msg("Run pre-bootstrap checks")

	// check is system systemd-aware
	if !isRunningSystemd() {
		return fmt.Errorf("systemd is not running")
	}

	// check root privileges
	if os.Geteuid() != 0 {
		return fmt.Errorf("root privileges required")
	}
	return nil
}

// installs agent binary
func installBin(config *bootstrapConfig) error {
	log.Info().Msg("Install agent")
	fromFilename := fmt.Sprintf("./%s", config.AgentBinaryName)
	toFilename := fmt.Sprintf("/usr/local/bin/%s", config.AgentBinaryName)

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
		log.Warn().Err(err).Msg("close source file failed, ignore it")
	}
	if err = to.Close(); err != nil {
		log.Warn().Err(err).Msg("close destination file failed, ignore it")
	}
	return nil
}

// creates systemd unit in system path
func createEnvironmentFile(config *bootstrapConfig) error {
	var envdir = "/etc/environment.d"
	log.Info().Msg("Create environment file")

	// check directory exists and create if not exists
	if _, err := os.Stat(envdir); os.IsNotExist(err) {
		err = os.Mkdir(envdir, os.ModeDir)
		if err != nil {
			return fmt.Errorf("create environment directory failed: %s ", err)
		}
	}

	// create environment config-file with proper permissions
	envfile := fmt.Sprintf("%s/%s.conf", envdir, config.AgentBinaryName)
	f, err := os.Create(envfile)
	if err != nil {
		return fmt.Errorf("create environment file failed: %s ", err)
	}
	if err = f.Chmod(0600); err != nil {
		log.Warn().Err(err).Msg("change file permissions failed, leave defaults")
	}

	// write content using template
	t, err := template.New("envconf").Parse(envFileTemplate)
	if err != nil {
		return fmt.Errorf("parse template failed: %s", err)
	}
	err = t.Execute(f, config)
	if err != nil {
		return fmt.Errorf("execute template failed: %s ", err)
	}

	if err = f.Close(); err != nil {
		log.Warn().Err(err).Msg("close file failed, ignore it")
	}

	return nil
}

// creates systemd unit in system path
func createSystemdUnit(config *bootstrapConfig) error {
	log.Info().Msg("Create systemd unit")
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
		log.Warn().Err(err).Msg("close file failed, ignore it")
	}
	return nil
}

// reloads systemd
func reloadSystemd() error {
	log.Info().Msg("Reload systemd")
	cmd := exec.Command("systemctl", "daemon-reload")
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("systemd reload failed: %s ", err)
	}

	log.Info().Msg("bootstrap: waiting until systemd daemon-reload to finish...")
	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("systemd reload failed: %s ", err)
	}
	return nil
}

// enables agent autostart
func enableAutostart(config *bootstrapConfig) error {
	log.Info().Msg("Enable autostart")

	servicename := fmt.Sprintf("%s.service", config.AgentBinaryName)
	cmd := exec.Command("systemctl", "enable", servicename)
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("enable agent service failed: %s ", err)
	}
	log.Info().Msg("bootstrap: waiting until systemd enables agent service...")

	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("systemd enable service failed: %s ", err)
	}
	return nil
}

// run agent systemd unit
func runAgent(config *bootstrapConfig) error {
	log.Info().Msg("Run agent")

	servicename := fmt.Sprintf("%s.service", config.AgentBinaryName)
	cmd := exec.Command("systemctl", "start", servicename)
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("start agent service failed: %s ", err)

	}
	log.Info().Msg("bootstrap: waiting until systemd starts agent service...")

	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("systemd starting service failed: %s ", err)
	}
	return nil
}

// delete self executable
func deleteSelf(config *bootstrapConfig) error {
	log.Info().Msg("Cleanup")
	filename := fmt.Sprintf("./%s", config.AgentBinaryName)
	return os.Remove(filename)
}

// bootstrapFailed signales bootstrap failed with error
func bootstrapFailed(e error) int {
	log.Error().Err(e).Msg("Stop bootstrap: %s")
	return 1
}

// bootstrapSuccessful signales bootstrap finished successfully
func bootstrapSuccessful() int {
	log.Info().Msg("Bootstrap successful")
	return 0
}

// isRunningSystemd checks whether the host was booted with systemd as its init system. This functions similarly to
// systemd's `sd_booted(3)`: internally, it checks whether /run/systemd/system/ exists and is a directory.
// http://www.freedesktop.org/software/systemd/man/sd_booted.html
func isRunningSystemd() bool {
	fi, err := os.Lstat("/run/systemd/system")
	if err != nil {
		return false
	}
	return fi.IsDir()
}
