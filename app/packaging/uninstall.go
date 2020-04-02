package packaging

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"os"
	"os/exec"
)

type UninstallConfig struct {
	BinaryName string
}

// RunUninstall is the main uninstall entry point
func RunUninstall(config *UninstallConfig) int {
	log.Info().Msg("Run uninstall")
	if err := preCheck(); err != nil {
		return uninstallFailed(err)
	}

	if err := stopAgent(config); err != nil {
		return uninstallFailed(err)
	}

	if err := removeServiceUnit(config); err != nil {
		return uninstallFailed(err)
	}

	if err := removeEnvConfig(config); err != nil {
		return uninstallFailed(err)
	}

	if err := removeBinary(config); err != nil {
		return uninstallFailed(err)
	}

	return uninstallSuccessful()
}

// stopAgent stops agent' systemd service
func stopAgent(c *UninstallConfig) error {
	log.Info().Msg("Stop agent")

	servicename := fmt.Sprintf("%s.service", c.BinaryName)
	cmd := exec.Command("systemctl", "stop", servicename)
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("stop agent service failed: %s ", err)

	}
	log.Info().Msg("uninstall: waiting until systemd stops agent service...")

	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("systemd stop service failed: %s ", err)
	}
	return nil
}

// removeServiceUnit removes systemd unit file
func removeServiceUnit(c *UninstallConfig) error {
	log.Info().Msg("Remove systemd unit")
	filename := fmt.Sprintf("/etc/systemd/system/%s.service", c.BinaryName)
	return os.Remove(filename)
}

// removeEnvConfig removes environment configuration file
func removeEnvConfig(c *UninstallConfig) error {
	log.Info().Msg("Remove environment file")
	filename := fmt.Sprintf("/etc/environment.d/%s.conf", c.BinaryName)
	return os.Remove(filename)
}

// removeBinary removes binary
func removeBinary(c *UninstallConfig) error {
	log.Info().Msg("Remove agent")
	filename := fmt.Sprintf("/usr/bin/%s", c.BinaryName)
	return os.Remove(filename)
}

// uninstallFailed signales uninstall failed with error
func uninstallFailed(e error) int {
	log.Error().Err(e).Msg("Uninstall failed: %s")
	return 1
}

// uninstallSuccessful signales bootstrap finished successfully
func uninstallSuccessful() int {
	log.Info().Msg("Uninstall successful")
	return 0
}
