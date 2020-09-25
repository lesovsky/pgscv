package packaging

import (
	"fmt"
	"github.com/barcodepro/pgscv/internal/log"
	"os"
	"os/exec"
	"path/filepath"
)

// RunUninstall is the main uninstall entry point
func RunUninstall() int {
	log.Info("Run uninstall")
	if err := preCheck(); err != nil {
		return uninstallFailed(err)
	}

	if err := stopAgent(); err != nil {
		return uninstallFailed(err)
	}

	if err := removeServiceUnit(); err != nil {
		return uninstallFailed(err)
	}

	if err := removeConfig(); err != nil {
		return uninstallFailed(err)
	}

	if err := removeBinary(); err != nil {
		return uninstallFailed(err)
	}

	return uninstallSuccessful()
}

// stopAgent stops agent' systemd service
func stopAgent() error {
	log.Info("Stop agent")

	cmd := exec.Command("systemctl", "stop", systemdServiceName)
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("stop agent service failed: %s ", err)

	}
	log.Info("uninstall: waiting until systemd stops agent service...")

	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("systemd stop service failed: %s ", err)
	}
	return nil
}

// removeServiceUnit removes systemd unit file
func removeServiceUnit() error {
	log.Info("Remove systemd unit")
	filename := fmt.Sprintf("%s/%s", defaultSystemdPathPrefix, systemdServiceName)
	return os.Remove(filename)
}

// removeConfig removes configuration file
func removeConfig() error {
	log.Info("Remove config file")
	return os.Remove(filepath.Clean(fmt.Sprintf("%s/%s.yaml", defaultConfigPathPrefix, defaultExecutableName)))
}

// removeBinary removes binary
func removeBinary() error {
	log.Info("Remove agent")
	return os.Remove(filepath.Clean(fmt.Sprintf("/usr/bin/%s", defaultExecutableName)))
}

// uninstallFailed signales uninstall failed with error
func uninstallFailed(e error) int {
	log.Errorln("Uninstall failed: ", e)
	return 1
}

// uninstallSuccessful signales bootstrap finished successfully
func uninstallSuccessful() int {
	log.Info("Uninstall successful")
	return 0
}
