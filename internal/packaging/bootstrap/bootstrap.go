package bootstrap

import (
	"fmt"
	"github.com/weaponry/pgscv/internal/log"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
)

const confFileTemplate = `api_key: "{{ .APIKey }}"
send_metrics_url: "{{ .SendMetricsURL }}"
autoupdate: {{ .AutoUpdate }}
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
ExecStart={{ .Bindir }}/{{ .ExecutableName }} --config-file={{ .ConfigFile }}

# Only kill the agent process
KillMode=control-group

# Wait reasonable amount of time for agent up/down
TimeoutSec=5

# Restart agent if it crashes
Restart=on-failure
RestartSec=10

# if agent leaks during long period of time, let him to be the first person for eviction
OOMScoreAdjust=1000

[Install]
WantedBy=multi-user.target
`

// Config describes bootstrap configuration
type Config struct {
	// File directories and paths
	ExecutableName string // Name of the executable
	Prefix         string // Prefix path where pgscv should be installed
	Installdir     string // Root directory where pgscv should be installed
	Bindir         string // Directory for executables
	ConfigFile     string // Path and filename of config file
	SystemdUnit    string // Path and name of systemd unit file
	// Settings of configuration file
	AutoStart                bool   // should be service auto-started by systemd?
	RunAsUser                string // run service using this user
	SendMetricsURL           string // URL of remote metric service
	AutoUpdateEnv            string // CLI input flag for control self-update setting
	AutoUpdate               bool   // should be service do self-update?
	APIKey                   string // API key of remote metric service
	DefaultPostgresPassword  string // default password used for connecting to Postgres services
	DefaultPgbouncerPassword string // default password used for connecting to Pgbouncer services
}

// Validate checks configuration and set default values.
func (c *Config) Validate() error {
	log.Infoln("Validate bootstrap configuration")

	if c.RunAsUser == "" {
		c.RunAsUser = "postgres"
	}

	_, err := user.Lookup(c.RunAsUser)
	if err != nil {
		return fmt.Errorf("specified user does not exists: %s ", err)
	}

	if c.SendMetricsURL == "" {
		return fmt.Errorf("PGSCV_SEND_METRICS_URL is not defined")
	}

	switch c.AutoUpdateEnv {
	case "y", "yes", "Yes", "YES", "t", "true", "True", "TRUE", "1":
		c.AutoUpdate = true
	case "n", "no", "No", "NO", "f", "false", "False", "FALSE", "0":
		c.AutoUpdate = false
	default:
		return fmt.Errorf("PGSCV_AUTOUPDATE is not defined, use 'true' or 'false'")
	}

	if c.APIKey == "" {
		return fmt.Errorf("PGSCV_API_KEY is not defined")
	}

	// Set default install path, filenames, etc.
	c.ExecutableName = "pgscv"
	c.Prefix = "/usr/local"
	c.Installdir = c.Prefix + "/pgscv"
	c.Bindir = c.Installdir + "/bin"
	c.ConfigFile = "/etc/pgscv.yaml"
	c.SystemdUnit = "/etc/systemd/system/pgscv.service"

	return nil
}

// RunBootstrap is the main bootstrap entry point
func RunBootstrap(config Config) int {
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
func installBin(config Config) error {
	log.Info("Install agent")

	err := createDirectoryTree(config)
	if err != nil {
		return err
	}

	fromFilename := fmt.Sprintf("./%s", config.ExecutableName)
	toFilename := fmt.Sprintf("%s/%s", config.Bindir, config.ExecutableName)

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

	uid, gid, err := getUserIDs(config.RunAsUser)
	if err != nil {
		return fmt.Errorf("get user uid/gid failed: %s", err)
	}

	err = os.Chown(toFilename, uid, gid)
	if err != nil {
		return fmt.Errorf("chown failed: %s", err)
	}
	err = os.Chmod(toFilename, 0755) // #nosec G302
	if err != nil {
		return fmt.Errorf("chmod 0755 failed: %s", err)
	}
	return nil
}

// createDirectoryTree creates required directories.
func createDirectoryTree(config Config) error {
	if !strings.HasPrefix(config.Prefix, "/") {
		return fmt.Errorf("root directory is not an absolute path")
	}

	uid, gid, err := getUserIDs(config.RunAsUser)
	if err != nil {
		return err
	}

	dirs := []string{
		config.Prefix + "/pgscv",
		config.Bindir,
	}

	for _, d := range dirs {
		err = os.Mkdir(d, 0755) // #nosec G301
		if err != nil {
			return err
		}

		err = os.Chown(d, uid, gid)
		if err != nil {
			return err
		}
	}

	return nil
}

// createConfigFile creates config file.
func createConfigFile(config Config) error {
	log.Info("Create config file")

	// Compile config file content using template.
	t, err := template.New("conf").Parse(confFileTemplate)
	if err != nil {
		return fmt.Errorf("parse template failed: %s", err)
	}

	uid, gid, err := getUserIDs(config.RunAsUser)
	if err != nil {
		return fmt.Errorf("failed get user's uid/gid: %s", err)
	}

	// create config-file with proper permissions
	f, err := os.Create(config.ConfigFile)
	if err != nil {
		return fmt.Errorf("create config file failed: %s", err)
	}

	if err = f.Chown(uid, gid); err != nil {
		return fmt.Errorf("change file ownership failed: %s", err)
	}

	if err = f.Chmod(0600); err != nil {
		return fmt.Errorf("change file permissions failed: %s", err)
	}

	// Write content to file.
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
func createSystemdUnit(config Config) error {
	log.Info("Create systemd unit")
	t, err := template.New("unit").Parse(unitTemplate)
	if err != nil {
		return fmt.Errorf("parse template failed: %s", err)
	}

	f, err := os.Create(config.SystemdUnit)
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

	cmd := exec.Command("systemctl", "enable", "pgscv.service")
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

	cmd := exec.Command("systemctl", "start", "pgscv.service")
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
	return os.Remove(filepath.Clean("./pgscv"))
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

	if uid < 0 || gid < 0 {
		return -1, -1, fmt.Errorf("negative uid or gid")
	}

	return uid, gid, nil
}
