package app

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog/log"
	"io"
	"os"
	"os/exec"
	"text/template"
)

const unitTemplate = `
[Unit]
Description=Scout is the Weaponry platform agent for PostgreSQL ecosystem
After=syslog.target network.target

[Service]
Type=simple

User=postgres
Group=postgres

Environment="PROJECTID={{ .ProjectId }}"
Environment="METRIC_SERVICE_BASE_URL=https://push.wpnr.brcd.pro"
Environment="SEND_INTERVAL=60s"

WorkingDirectory=~

# Start the agent process
ExecStart=/usr/bin/scout

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
	ProjectId int64 `json:"project_id"`
	AutoStart bool  `json:"autostart"`
}

func newBootstrapConfig(configHash string) (*bootstrapConfig, error) {
	// parse confighash string to config struct
	data, err := base64.StdEncoding.DecodeString(configHash)
	if err != nil {
		return nil, fmt.Errorf("decode failed: %s", err)
	}
	var c bootstrapConfig
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("json unmarshalling failed: %s", err)

	}
	return &c, nil
}

// RunBootstrap is the main bootstrap entry point
func RunBootstrap(configHash string) int {
	log.Info().Msg("Running bootstrap")
	if err := preCheck(configHash); err != nil {
		return bootstrapFailed(err)
	}

	config, err := newBootstrapConfig(configHash)
	if err != nil {
		return bootstrapFailed(err)
	}

	if err := installBin(); err != nil {
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

// run pre-bootstrap checks
func preCheck(configHash string) error {
	log.Info().Msg("pre-bootstrap checks")
	if configHash == "" {
		return fmt.Errorf("empty config passed")
	}

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
func installBin() error {
	log.Info().Msg("install agent")
	from, err := os.Open("./scout")
	if err != nil {
		return fmt.Errorf("open file failed: %s", err)

	}
	to, err := os.OpenFile("/usr/bin/scout", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0755)
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
func createSystemdUnit(config *bootstrapConfig) error {
	log.Info().Msg("create systemd unit")
	t, err := template.New("unit").Parse(unitTemplate)
	if err != nil {
		return fmt.Errorf("parse template failed: %s", err)
	}

	f, err := os.Create("/etc/systemd/system/scout.service")
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
	log.Info().Msg("reload systemd")
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
func enableAutostart() error {
	log.Info().Msg("enable autostart")
	cmd := exec.Command("systemctl", "enable", "scout.service")
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
func runAgent() error {
	log.Info().Msg("run agent")
	cmd := exec.Command("systemctl", "start", "scout.service")
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
func deleteSelf() error {
	log.Info().Msg("cleanup")
	return os.Remove("scout")
}

// bootstrapFailed signales bootstrap failed with error
func bootstrapFailed(e error) int {
	log.Error().Err(e).Msg("stop bootstrap: %s")
	return 1
}

// bootstrapSuccessful signales bootstrap finished successfully
func bootstrapSuccessful() int {
	log.Info().Msg("bootstrap successful")
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
