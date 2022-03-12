package service

import (
	"fmt"
	"github.com/lesovsky/pgscv/internal/log"
	"github.com/lesovsky/pgscv/internal/model"
	"gopkg.in/yaml.v2"
	"net"
	"os"
	"path/filepath"
	"strings"
)

// discoverPatroni analyze process properties to guess it is a Patroni process otr not.
// Note: it unable to discover through environment variables, only config file.
func discoverPatroni(pid int32, cmdline string, cwd string) (Service, bool, error) {
	log.Debugf("auto-discovery [patroni]: analyzing process with pid %d", pid)

	if len(cmdline) == 0 {
		return Service{}, true, fmt.Errorf("patroni cmdline is empty")
	}

	// Parse cmdline and looking for path to Patroni YAML configuration file.
	configFilePath := parsePatroniCmdline(cmdline, cwd)

	if configFilePath == "" {
		return Service{}, true, fmt.Errorf("patroni YAML configuration file not found")
	}

	// Extract from YAML configuration connection settings to REST API.
	conninfo, err := newPatroniConninfo(configFilePath)
	if err != nil {
		return Service{}, true, err
	}

	var baseURL string
	if conninfo.ssl {
		baseURL = fmt.Sprintf("https://%s:%s", conninfo.host, conninfo.port)
	} else {
		baseURL = fmt.Sprintf("http://%s:%s", conninfo.host, conninfo.port)
	}

	s := Service{
		ServiceID: model.ServiceTypePatroni + ":" + conninfo.port,
		ConnSettings: ConnSetting{
			ServiceType: model.ServiceTypePatroni,
			BaseURL:     baseURL,
		},
		Collector: nil,
	}

	log.Debugf("auto-discovery: patroni service has been found, pid %d, available through %s:%s", pid, conninfo.host, conninfo.port)
	return s, false, nil
}

// parsePatroniCmdline parses Patroni cmdline for config file and returns it if found.
func parsePatroniCmdline(cmdline string, cwd string) string {
	parts := strings.Fields(cmdline)

	var configFilePath string

	for _, s := range parts[1:] {
		if strings.HasSuffix(s, ".yml") || strings.HasSuffix(s, ".yaml") {
			configFilePath = s
			break
		}
	}

	// Return value if it is an absolute path.
	if strings.HasPrefix(configFilePath, "/") || configFilePath == "" {
		return configFilePath
	}

	// For relative paths, prepend value with current working directory.
	return cwd + "/" + strings.TrimLeft(configFilePath, "./")
}

// patroniConninfo defines connection settings for Patroni service.
type patroniConninfo struct {
	host string
	port string
	ssl  bool
}

// newPatroniConninfo parses content of YAML configuration, looking for 'restapi.listen' and returns its value.
func newPatroniConninfo(cfgFile string) (patroniConninfo, error) {
	type restapi struct {
		Listen   string `yaml:"listen"`
		Certfile string `yaml:"certfile"`
	}
	type patroniConfig struct {
		Restapi restapi `yaml:"restapi"`
	}

	content, err := os.ReadFile(filepath.Clean(cfgFile))
	if err != nil {
		return patroniConninfo{}, err
	}

	config := &patroniConfig{}

	err = yaml.Unmarshal(content, config)
	if err != nil {
		return patroniConninfo{}, err
	}

	host, port, err := parseListenString(config.Restapi.Listen)
	if err != nil {
		return patroniConninfo{}, err
	}

	return patroniConninfo{
		host: host,
		port: port,
		ssl:  config.Restapi.Certfile != "",
	}, nil
}

// parseListenString parses value of 'restapi.listen' and returns host and port values.
func parseListenString(s string) (string, string, error) {
	if s == "" {
		return "", "", fmt.Errorf("patroni configuration option 'restapi.listen' not found")
	}

	if s == "::" {
		return "[::1]", "8008", nil
	}

	parts := strings.Split(s, ":")

	var addr, port string
	var ip net.IP

	if len(parts) != 1 {
		ip = net.ParseIP(strings.Join(parts[0:len(parts)-1], ":"))
		port = parts[len(parts)-1]
	} else {
		ip = net.ParseIP(parts[0])
		port = "8008"
	}

	// Convert 'unspecified' address to loopback. Wraps IPv6 addresses into square brackets (required for net/http).
	if ip.Equal(net.IPv4zero) {
		addr = "127.0.0.1"
	} else if ip.Equal(net.IPv6unspecified) {
		addr = fmt.Sprintf("[%s]", net.IPv6loopback.String())
	} else {
		if ip.To4() != nil {
			addr = ip.String()
		} else {
			addr = fmt.Sprintf("[%s]", ip.String())
		}
	}

	return addr, port, nil
}
