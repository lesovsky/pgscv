package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/prometheus/common/log"
	"io"
	"os"
	"os/exec"
	"text/template"
)

const unitTemplate = `
[Unit]
Description=pgSCV is the Weaponry platform agent for PostgreSQL ecosystem
After=syslog.target network.target

[Service]
Type=simple

User=postgres
Group=postgres

Environment="PGSCV_PROJECTID={{ .ProjectId }}"
Environment="PGSCV_METRIC_GATEWAY=push.wpnr.brcd.pro"
Environment="PGSCV_SEND_INTERVAL=60s"

WorkingDirectory=~

# Start the pgscv process
ExecStart=/usr/bin/pgscv

# Only kill the pgscv process
KillMode=process

# Wait reasonable amount of time for pgSCV up/down
TimeoutSec=5

# Restart pgSCV if it crashes
Restart=on-failure

# pgSCV might leak during long period of time, let him to be the first person for eviction
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

// doBootstrap is the main bootstrap entry point
func doBootstrap(configHash string) int {
	log.Info("Running bootstrap")
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

	if err := runPgscv(); err != nil {
		return bootstrapFailed(err)
	}

	if err := deleteSelf(); err != nil {
		return bootstrapFailed(err)
	}

	return bootstrapSuccessful()
}

// run pre-bootstrap checks
func preCheck(configHash string) error {
	log.Info("pre-bootstrap checks")
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

// installs pgscv binary
func installBin() error {
	log.Info("install pgscv")
	from, err := os.Open("./pgscv")
	if err != nil {
		return fmt.Errorf("open pgscv file failed: %s", err)

	}
	to, err := os.OpenFile("/usr/bin/pgscv", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0755)
	if err != nil {
		return fmt.Errorf("open destination file failed: %s", err)
	}
	_, err = io.Copy(to, from)
	if err != nil {
		return fmt.Errorf("copy pgscv file failed: %s", err)
	}
	if err = from.Close(); err != nil {
		log.Warnf("close source pgscv file failed: %s, ignore it ", err)
	}
	if err = to.Close(); err != nil {
		log.Warnf("close destination pgscv file failed: %s, ignore it ", err)
	}
	return nil
}

// creates systemd unit in system path
func createSystemdUnit(config *bootstrapConfig) error {
	log.Info("create systemd unit")
	t, err := template.New("unit").Parse(unitTemplate)
	if err != nil {
		return fmt.Errorf("parse template failed: %s", err)
	}

	f, err := os.Create("/etc/systemd/system/pgscv.service")
	if err != nil {
		return fmt.Errorf("create file failed: %s ", err)
	}

	err = t.Execute(f, config)
	if err != nil {
		return fmt.Errorf("execute template failed: %s ", err)
	}

	if err = f.Close(); err != nil {
		log.Warnf("close file failed: %s, ignore it ", err)
	}
	return nil
}

// reloads systemd
func reloadSystemd() error {
	log.Info("reload systemd")
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

// enables pgscv autostart
func enableAutostart() error {
	log.Info("enable autostart")
	cmd := exec.Command("systemctl", "enable", "pgscv.service")
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("enable pgscv service failed: %s ", err)
	}
	log.Info("bootstrap: waiting until systemd enables pgscv service...")

	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("systemd enable service failed: %s ", err)
	}
	return nil
}

// run pgscv systemd unit
func runPgscv() error {
	log.Info("run pgscv")
	cmd := exec.Command("systemctl", "start", "pgscv.service")
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("start pgscv service failed: %s ", err)

	}
	log.Info("bootstrap: waiting until systemd starts pgscv service...")

	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("systemd starting service failed: %s ", err)
	}
	return nil
}

// delete self executable
func deleteSelf() error {
	log.Info("cleanup")
	return os.Remove("pgscv")
}

// bootstrapFailed signales bootstrap failed with error
func bootstrapFailed(e error) int {
	log.Errorf("stop bootstrap: %s", e)
	return 1
}

// bootstrapSuccessful signales bootstrap finished successfully
func bootstrapSuccessful() int {
	log.Info("bootstrap successful")
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
