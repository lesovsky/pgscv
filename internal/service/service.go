package service

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/process"
	"github.com/weaponry/pgscv/internal/collector"
	"github.com/weaponry/pgscv/internal/http"
	"github.com/weaponry/pgscv/internal/log"
	"github.com/weaponry/pgscv/internal/model"
	"github.com/weaponry/pgscv/internal/store"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultHost              = "127.0.0.1"
	defaultPgbouncerPort     = 6432
	defaultPostgresUsername  = "pgscv"
	defaultPostgresDbname    = "postgres"
	defaultPgbouncerUsername = "pgscv"
	defaultPgbouncerDbname   = "pgbouncer"
)

// Service struct describes service - the target from which should be collected metrics.
type Service struct {
	// Service identifier is unique key across all monitored services and used to distinguish services of the same type
	// running on the single host (two Postgres services running on the same host but listening on different ports).
	// Hence not to mix their metrics the ServiceID is introduced and attached to metrics as "sid" label:
	// metric_xact_commits{database="test", sid="postgres:5432"} -- metric from the first postgres running on 5432 port
	// metric_xact_commits{database="test", sid="postgres:5433"} -- metric from the second postgres running on 5433 port
	ServiceID string
	// Connection settings required for connecting to the service.
	ConnSettings ConnSetting
	// Prometheus-based metrics collector associated with the service. Each 'service' has its own dedicated collector instance
	// which implements a service-specific set of metric collectors.
	Collector Collector
	// TotalErrors represents total number of times where service's health checks failed. When errors limit is reached service
	// removed from the repo.
	TotalErrors int
}

// Config defines service's configuration.
type Config struct {
	RuntimeMode   int
	NoTrackMode   bool
	ConnDefaults  map[string]string `yaml:"defaults"` // Defaults
	ConnsSettings ConnsSettings
	// DatabasesRE defines regexp with databases from which builtin metrics should be collected.
	DatabasesRE        *regexp.Regexp
	DisabledCollectors []string
	// CollectorsSettings defines all collector settings propagated from main YAML configuration.
	CollectorsSettings model.CollectorsSettings
}

// Exporter is an interface for prometheus.Collector.
type Collector interface {
	Describe(chan<- *prometheus.Desc)
	Collect(chan<- prometheus.Metric)
}

// connectionParams is the set of parameters that may be required when constructing connection string.
// For example, this struct describes the postmaster.pid representation https://www.postgresql.org/docs/current/storage-file-layout.html
type connectionParams struct {
	pid               int    // process id
	datadirPath       string // instance data directory path
	startTs           int64  // postmaster start timestamp
	unixSocketDirPath string // UNIX-domain socket directory path
	listenAddr        string // first valid listen_address (IP address or *, or empty if not listening on TCP)
	listenPort        int    // port number
	// ... other stuff we're not interested in
}

// Repository is the repository with services.
type Repository struct {
	sync.RWMutex                    // protect concurrent access
	Services     map[string]Service // service repo store
}

// NewRepository creates new services repository.
func NewRepository() *Repository {
	return &Repository{
		Services: make(map[string]Service),
	}
}

/* Public wrapper-methods of Repository */

// GetService is a public wrapper on getService method.
func (repo *Repository) GetService(id string) Service {
	return repo.getService(id)
}

// TotalServices is a public wrapper on TotalServices method.
func (repo *Repository) TotalServices() int {
	return repo.totalServices()
}

// GetServiceIDs is a public wrapper on GetServiceIDs method.
func (repo *Repository) GetServiceIDs() []string {
	return repo.getServiceIDs()
}

// AddServicesFromConfig is a public wrapper on AddServicesFromConfig method.
func (repo *Repository) AddServicesFromConfig(config Config) {
	repo.addServicesFromConfig(config)
}

// SetupServices is a public wrapper on SetupServices method.
func (repo *Repository) SetupServices(config Config) error {
	return repo.setupServices(config)
}

// StartBackgroundDiscovery is a public wrapper on StartBackgroundDiscovery method.
func (repo *Repository) StartBackgroundDiscovery(ctx context.Context, config Config) {
	repo.startBackgroundDiscovery(ctx, config)
}

/* Private methods of Repository */

// addService adds service to the repo.
func (repo *Repository) addService(s Service) {
	repo.Lock()
	repo.Services[s.ServiceID] = s
	repo.Unlock()
}

// getService returns the service from repo with specified ID.
func (repo *Repository) getService(id string) Service {
	repo.RLock()
	s := repo.Services[id]
	repo.RUnlock()
	return s
}

// markServiceFailed increments total number of health check errors.
func (repo *Repository) markServiceFailed(id string) {
	repo.Lock()
	s := repo.Services[id]
	s.TotalErrors++
	repo.Services[id] = s
	repo.Unlock()
}

// getServiceStatus returns total number of errors (failed health checks).
func (repo *Repository) getServiceStatus(id string) int {
	repo.RLock()
	n := repo.Services[id].TotalErrors
	repo.RUnlock()
	return n
}

// markServiceHealthy resets health check errors counter to zero.
func (repo *Repository) markServiceHealthy(id string) {
	repo.Lock()
	s := repo.Services[id]
	s.TotalErrors = 0
	repo.Services[id] = s
	repo.Unlock()
}

// removeService removes service from the repo.
func (repo *Repository) removeService(id string) {
	repo.Lock()
	delete(repo.Services, id)
	repo.Unlock()
}

// totalServices returns the number of services registered in the repo.
func (repo *Repository) totalServices() int {
	repo.RLock()
	var size = len(repo.Services)
	repo.RUnlock()
	return size
}

// getServiceIDs returns slice of services' IDs in the repo.
func (repo *Repository) getServiceIDs() []string {
	var serviceIDs = make([]string, 0, repo.totalServices())
	repo.RLock()
	for i := range repo.Services {
		serviceIDs = append(serviceIDs, i)
	}
	repo.RUnlock()
	return serviceIDs
}

// addServicesFromConfig reads info about services from the config file and fulfill the repo.
func (repo *Repository) addServicesFromConfig(config Config) {
	log.Debug("config: add services from configuration")

	// Always add system service.
	repo.addService(Service{ServiceID: "system:0", ConnSettings: ConnSetting{ServiceType: model.ServiceTypeSystem}})
	log.Info("registered new service [system:0]")

	// Sanity check, but basically should be always passed.
	if config.ConnsSettings == nil {
		log.Warn("connection settings for service are not defined, do nothing")
		return
	}

	// Check all passed connection settings and try to connect using them. In case of success, create a 'Service' instance
	// in the repo.
	for k, cs := range config.ConnsSettings {
		// each ConnSetting struct is used for
		//   1) doing connection;
		//   2) getting connection properties to define service-specific parameters.
		pgconfig, err := pgx.ParseConfig(cs.Conninfo)
		if err != nil {
			log.Warnf("%s: %s, skip", cs.Conninfo, err)
			continue
		}

		// Check connection using created *ConnConfig, go next if connection failed.
		db, err := store.NewWithConfig(pgconfig)
		if err != nil {
			log.Warnf("%s: %s, skip", cs.Conninfo, err)
			continue
		}
		db.Close()

		// Connection was successful, create 'Service' struct with service-related properties and add it to service repo.
		s := Service{
			ServiceID:    k,
			ConnSettings: cs,
			Collector:    nil,
		}

		// Use entry key as ServiceID unique identifier.
		repo.addService(s)

		log.Infof("registered new service [%s]", s.ServiceID)

		var msg string
		if s.ConnSettings.ServiceType == model.ServiceTypePatroni {
			msg = fmt.Sprintf("service [%s] available through: %s", s.ServiceID, s.ConnSettings.BaseURL)
		} else {
			msg = fmt.Sprintf("service [%s] available through: %s@%s:%d/%s", s.ServiceID, pgconfig.User, pgconfig.Host, pgconfig.Port, pgconfig.Database)
		}
		log.Debugln(msg)
	}
}

// startBackgroundDiscovery looking for services and add them to the repo.
func (repo *Repository) startBackgroundDiscovery(ctx context.Context, config Config) {
	log.Debug("starting background auto-discovery loop")

	// add pseudo-service for system metrics
	repo.addService(Service{ServiceID: "system:0", ConnSettings: ConnSetting{ServiceType: model.ServiceTypeSystem}})
	log.Infoln("auto-discovery: service added [system:0]")

	for {
		if err := repo.lookupServices(config); err != nil {
			log.Warnf("auto-discovery: services lookup failed: %s; skip", err)
			continue
		}
		if err := repo.setupServices(config); err != nil {
			log.Warnf("auto-discovery: services setup failed: %s; skip", err)
			continue
		}

		// Perform health check for services with remote endpoints (e.g. Postgres or Pgbouncer). Services which continuously
		// don't respond are removed from the repo (but if they appear later they will be discovered again).
		repo.healthcheckServices()

		// Sleep until timeout or exit if context canceled.
		select {
		case <-time.After(60 * time.Second):
			continue
		case <-ctx.Done():
			log.Info("exit signaled, stop auto-discovery")
			return
		}
	}
}

// lookupServices scans PIDs and looking for required services
func (repo *Repository) lookupServices(config Config) error {
	log.Debug("auto-discovery: looking up for new services...")

	pids, err := process.Pids()
	if err != nil {
		return err
	}

	// walk through the pid list and looking for the processes with appropriate names
	for _, pid := range pids {

		// Check process, and get its properties.
		name, cwd, cmdline, skip := checkProcessProperties(pid)
		if skip {
			continue
		}

		var service Service
		var err error

		switch {
		case name == "postgres":
			service, err = discoverPostgres(pid, cwd, config)
		case name == "pgbouncer":
			service, err = discoverPgbouncer(pid, cmdline, config)
		case strings.HasPrefix(name, "python"):
			service, skip, err = discoverPatroni(pid, cmdline, cwd)
		default:
			continue
		}

		if err != nil {
			log.Warnf("auto-discovery [%s]: discovery failed: %s; skip", name, err)
			continue
		}

		if skip {
			continue
		}

		// Check service is not present in the repo.
		if s := repo.getService(service.ServiceID); s.ServiceID == service.ServiceID {
			log.Debugf("auto-discovery [%s]: service [%s] already in the repository, skip", name, s.ServiceID)
			continue
		}

		// Add postgresql service to the repo.
		repo.addService(service)

		log.Infof("auto-discovery [%s]: service added [%s]", name, service.ServiceID)
	}
	return nil
}

// setupServices attaches metrics exporters to the services in the repo.
func (repo *Repository) setupServices(config Config) error {
	log.Debug("config: setting up services")

	for _, id := range repo.getServiceIDs() {
		var service = repo.getService(id)
		if service.Collector == nil {
			factories := collector.Factories{}
			collectorConfig := collector.Config{
				NoTrackMode: config.NoTrackMode,
				ServiceType: service.ConnSettings.ServiceType,
				ConnString:  service.ConnSettings.Conninfo,
				Settings:    config.CollectorsSettings,
				DatabasesRE: config.DatabasesRE,
			}

			switch service.ConnSettings.ServiceType {
			case model.ServiceTypeSystem:
				factories.RegisterSystemCollectors(config.DisabledCollectors)
			case model.ServiceTypePostgresql:
				factories.RegisterPostgresCollectors(config.DisabledCollectors)
			case model.ServiceTypePgbouncer:
				factories.RegisterPgbouncerCollectors(config.DisabledCollectors)
			case model.ServiceTypePatroni:
				factories.RegisterPatroniCollectors(config.DisabledCollectors)
				collectorConfig.BaseURL = service.ConnSettings.BaseURL
			default:
				continue
			}

			mc, err := collector.NewPgscvCollector(service.ServiceID, factories, collectorConfig)
			if err != nil {
				return err
			}
			service.Collector = mc

			// Register collector.
			prometheus.MustRegister(service.Collector)

			// Put updated service into repo.
			repo.addService(service)
			log.Debugf("service configured [%s]", id)
		}
	}

	return nil
}

// healthcheckServices performs services health checks and remove those who don't respond too long
func (repo *Repository) healthcheckServices() {
	log.Debug("services healthcheck started")

	// Remove service after 10 failed health checks.
	var errorThreshold = 10

	for _, id := range repo.getServiceIDs() {
		service := repo.getService(id)
		totalErrors := repo.getServiceStatus(id)
		var err error

		switch service.ConnSettings.ServiceType {
		case model.ServiceTypePostgresql, model.ServiceTypePgbouncer:
			err = attemptConnect(service.ConnSettings.Conninfo)
		case model.ServiceTypePatroni:
			err = attemptRequest(service.ConnSettings.BaseURL)
		default:
			continue
		}

		// Process errors if any.
		if err != nil {
			totalErrors++
			if totalErrors < errorThreshold {
				repo.markServiceFailed(id)
				log.Warnf("service [%s] failed: tries remain %d/%d", id, totalErrors, errorThreshold)
			} else {
				// Unregister collector and remove the service.
				if repo.Services[id].Collector != nil {
					prometheus.Unregister(repo.Services[id].Collector)
				}

				repo.removeService(id)
				log.Errorf("service [%s] removed: too many failures %d/%d", id, totalErrors, errorThreshold)
			}
		}
	}

	log.Debug("services healthcheck finished")
}

// discoverPostgres reads postmaster.pid stored in data directory.
// Using postmaster.pid data construct "conninfo" string and test it through making a connection.
func discoverPostgres(pid int32, cwd string, config Config) (Service, error) {
	log.Debugf("auto-discovery [postgres]: analyzing process with pid %d", pid)

	var err error

	// Postgres always use data directory as current working directory.
	// Use it for find postmaster.pid.
	connParams, err := newPostgresConnectionParams(cwd + "/postmaster.pid")
	if err != nil {
		return Service{}, err
	}

	// Construct the connection string using the data from postmaster.pid and user-defined defaults.
	// Depending on configured Postgres there can be UNIX-based or TCP-based connection string
	var connString string
	for _, v := range []bool{true, false} {
		connString = newPostgresConnectionString(connParams, config.ConnDefaults, v)
		err = attemptConnect(connString)
		if err != nil {
			connString = ""
			continue
		}

		// no need to continue because connection with created connString was successful
		break
	}

	if connString == "" || err != nil {
		return Service{}, err
	}

	s := Service{
		ServiceID:    model.ServiceTypePostgresql + ":" + strconv.Itoa(connParams.listenPort),
		ConnSettings: ConnSetting{ServiceType: model.ServiceTypePostgresql, Conninfo: connString},
		Collector:    nil,
	}

	log.Debugf("auto-discovery [postgres]: service has been found, pid %d, available through %s", pid, connString)
	return s, nil
}

// newPostgresConnectionParams reads connection parameters from postmaster.pid
func newPostgresConnectionParams(pidFilePath string) (connectionParams, error) {
	p := connectionParams{}
	content, err := os.ReadFile(filepath.Clean(pidFilePath))
	if err != nil {
		return p, err
	}

	reader := bufio.NewReader(bytes.NewBuffer(content))
	for i := 0; ; i++ {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			return p, err
		}
		switch i {
		case 0:
			p.pid, err = strconv.Atoi(string(line))
			if err != nil {
				return p, err
			}
		case 1:
			p.datadirPath = string(line)
		case 2:
			p.startTs, err = strconv.ParseInt(string(line), 10, 64)
			if err != nil {
				return p, err
			}
		case 3:
			p.listenPort, err = strconv.Atoi(string(line))
			if err != nil {
				return p, err
			}
		case 4:
			p.unixSocketDirPath = string(line)
		case 5:
			if string(line) == "*" {
				p.listenAddr = defaultHost
			} else {
				p.listenAddr = string(line)
			}
		}
	}
	return p, nil
}

// newPostgresConnectionString creates special connection string for connecting to Postgres using passed connection parameters.
func newPostgresConnectionString(connParams connectionParams, defaults map[string]string, unix bool) string {
	var password, connString string
	var username = defaultPostgresUsername
	var dbname = defaultPostgresDbname

	if _, ok := defaults["postgres_username"]; ok {
		username = defaults["postgres_username"]
	}

	if _, ok := defaults["postgres_dbname"]; ok {
		dbname = defaults["postgres_dbname"]
	}

	if _, ok := defaults["postgres_password"]; ok {
		password = defaults["postgres_password"]
	}

	connString = "application_name=pgscv"

	if unix && connParams.unixSocketDirPath != "" {
		connString = fmt.Sprintf("%s host=%s", connString, connParams.unixSocketDirPath)
	} else if !unix && connParams.listenAddr != "" {
		connString = fmt.Sprintf("%s host=%s", connString, connParams.listenAddr)
	}

	if connParams.listenPort > 0 {
		connString = fmt.Sprintf("%s port=%d", connString, connParams.listenPort)
	}

	connString = fmt.Sprintf("%s user=%s dbname=%s", connString, username, dbname)

	if password != "" {
		connString = fmt.Sprintf("%s password=%s", connString, password)
	}

	return connString
}

// discoverPgbouncer check passed process is it a Pgbouncer process or not.
func discoverPgbouncer(pid int32, cmdline string, config Config) (Service, error) {
	log.Debugf("auto-discovery [pgbouncer]: analyzing process with pid %d", pid)

	if len(cmdline) == 0 {
		return Service{}, fmt.Errorf("pgbouncer cmdline is empty")
	}

	// extract config file location from cmdline
	configFilePath := parsePgbouncerCmdline(cmdline)

	// parse ini file
	connParams, err := parsePgbouncerIniFile(configFilePath)
	if err != nil {
		return Service{}, err
	}

	connString := newPgbouncerConnectionString(connParams, config.ConnDefaults)

	if err := attemptConnect(connString); err != nil {
		return Service{}, err
	}

	s := Service{
		ServiceID:    model.ServiceTypePgbouncer + ":" + strconv.Itoa(connParams.listenPort),
		ConnSettings: ConnSetting{ServiceType: model.ServiceTypePgbouncer, Conninfo: connString},
		Collector:    nil,
	}

	log.Debugf("auto-discovery: pgbouncer service has been found, pid %d, available through %s:%d", pid, connParams.listenAddr, connParams.listenPort)
	return s, nil
}

// parsePgbouncerIniFile reads pgbouncer's config ini file and returns connection parameters required for constructing connection string.
func parsePgbouncerIniFile(iniFilePath string) (connectionParams, error) {
	// read the content of inifile
	file, err := os.Open(filepath.Clean(iniFilePath))
	if err != nil {
		return connectionParams{}, err
	}
	defer func() { _ = file.Close() }()

	var paramName, paramValue string
	var connParams connectionParams

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			log.Warnln("an error occurred during scan: ", err)
			continue
		}
		line := scanner.Text()

		// skip comments and empty lines
		if strings.HasPrefix(line, ";") || strings.HasPrefix(line, "#") || len(line) == 0 {
			continue
		}

		line = strings.Replace(line, " ", "", -1)
		vals := strings.Split(line, "=")
		if len(vals) != 2 {
			// if parameter is not set it means default valus is used, can skip that line
			continue
		}

		// looking for listen address and port settings, use them as connection settings
		paramName, paramValue = vals[0], vals[1]
		switch paramName {
		case "listen_addr":
			connParams.listenAddr = strings.Split(paramValue, ",")[0] // take first address
			if connParams.listenAddr == "*" {
				connParams.listenAddr = defaultHost
			}
		case "listen_port":
			connParams.listenPort, err = strconv.Atoi(paramValue)
			if err != nil {
				return connectionParams{}, err
			}
		case "unix_socket_dir":
			connParams.unixSocketDirPath = paramValue
		}
	}

	// set defaults in case of empty values, for more details see pgbouncer.ini reference https://www.pgbouncer.org/config.html
	if connParams.unixSocketDirPath == "" {
		connParams.unixSocketDirPath = "/tmp"
	}
	if connParams.listenPort == 0 {
		connParams.listenPort = defaultPgbouncerPort
	}

	return connParams, nil
}

// newPgbouncerConnectionString creates special connection string for connecting to Pgbouncer using passed connection parameters.
func newPgbouncerConnectionString(connParams connectionParams, defaults map[string]string) string {
	var password, connString string
	var username = defaultPgbouncerUsername

	if _, ok := defaults["pgbouncer_username"]; ok {
		username = defaults["pgbouncer_username"]
	}

	if _, ok := defaults["pgbouncer_password"]; ok {
		password = defaults["pgbouncer_password"]
	}

	connString = "application_name=pgscv"

	if connParams.listenAddr != "" {
		connString = fmt.Sprintf("%s host=%s", connString, connParams.listenAddr)
	} else if connParams.unixSocketDirPath != "" {
		connString = fmt.Sprintf("%s host=%s", connString, connParams.unixSocketDirPath)
	}

	if connParams.listenPort > 0 {
		connString = fmt.Sprintf("%s port=%d", connString, connParams.listenPort)
	}

	connString = fmt.Sprintf("%s user=%s dbname=%s", connString, username, defaultPgbouncerDbname)

	if password != "" {
		connString = fmt.Sprintf("%s password=%s", connString, password)
	}

	return connString
}

// attemptConnect tries to make a real connection using passed connection string.
func attemptConnect(connString string) error {
	log.Debugln("making test connection: ", connString)
	db, err := store.New(connString)
	if err != nil {
		return err
	}

	db.Close()
	log.Debug("test connection success")

	return nil
}

// attemptRequest tries to make a real HTTP request using passed URL string.
func attemptRequest(baseurl string) error {
	url := baseurl + "/health"
	log.Debugln("making test http request: ", url)

	var client = http.NewClient(http.ClientConfig{Timeout: time.Second})

	if strings.HasPrefix(url, "https://") {
		client.EnableTLSInsecure()
	}

	resp, err := client.Get(url) // #nosec G107
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad response: %s", resp.Status)
	}

	return nil
}

// parsePgbouncerCmdline parses pgbouncer's cmdline and extract config file location.
func parsePgbouncerCmdline(cmdline string) string {
	parts := strings.Fields(cmdline)

	// For extracting config file from cmdline we should skip first argument (pgbouncer executable) and skip all arguments
	// which starting with '-' symbol. See test function for examples.

	for _, s := range parts[1:] {
		if !strings.HasPrefix(s, "-") {
			return s
		}
	}
	return ""
}

// checkProcessProperties check process properties and returns necessary properties if process valid.
func checkProcessProperties(pid int32) (string, string, string, bool) {
	proc, err := process.NewProcess(pid)
	if err != nil {
		log.Debugf("auto-discovery: create process object for pid %d failed: %s; skip", pid, err)
		return "", "", "", true
	}

	ppid, err := proc.Ppid()
	if err != nil {
		log.Debugf("auto-discovery: get parent pid for pid %d failed: %s; skip", pid, err)
		return "", "", "", true
	}

	// Skip processes which are not children of init.
	if ppid != 1 {
		return "", "", "", true
	}

	name, err := proc.Name()
	if err != nil {
		log.Debugf("auto-discovery: read name for pid %d failed: %s; skip", pid, err)
		return "", "", "", true
	}

	// Skip processes which are not Postgres, Pgbouncer or Python.
	if name != "postgres" && name != "pgbouncer" && !strings.HasPrefix(name, "python") {
		return "", "", "", true
	}

	cwd, err := proc.Cwd()
	if err != nil {
		log.Infof("auto-discovery: read cwd for pid %d failed: %s; skip", pid, err)
		return "", "", "", true
	}

	cmdline, err := proc.Cmdline()
	if err != nil {
		log.Infof("auto-discovery: read cmdline for pid %d failed: %s; skip", pid, err)
		return "", "", "", true
	}

	return name, cwd, cmdline, false
}

// stringsContains returns true if array of strings contains specific string
func stringsContains(ss []string, s string) bool {
	for _, val := range ss {
		if val == s {
			return true
		}
	}
	return false
}
