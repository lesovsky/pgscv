package packaging

import (
	"fmt"
	"github.com/barcodepro/pgscv/internal/log"
	"os"
)

const (
	defaultExecutableName = "pgscv"
	systemdServiceName    = "pgscv.service"

	defaultConfigPathPrefix  = "/etc"
	defaultSystemdPathPrefix = "/etc/systemd/system"
)

// run pre-bootstrap checks
func preCheck() error {
	log.Info("run pre-flight checks")

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
