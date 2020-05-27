package app

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/barcodepro/pgscv/service/internal/log"
	"github.com/barcodepro/pgscv/service/model"
	"github.com/barcodepro/pgscv/service/store"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/process"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

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

// ServiceRepo is the repository with services.
type ServiceRepo struct {
	sync.RWMutex                          // protect concurrent access
	Services     map[string]model.Service // service repo store
}

// NewServiceRepo creates new services repository.
func NewServiceRepo() *ServiceRepo {
	return &ServiceRepo{
		Services: make(map[string]model.Service),
	}
}

// addService adds service to the repo.
func (repo *ServiceRepo) addService(id string, s model.Service) {
	repo.Lock()
	repo.Services[id] = s
	repo.Unlock()
}

// getService returns the service from repo with specified ID.
func (repo *ServiceRepo) getService(id string) model.Service {
	repo.RLock()
	s := repo.Services[id]
	repo.RUnlock()
	return s
}

// removeService removes service from the repo.
func (repo *ServiceRepo) removeServiceByServiceID(id string) {
	repo.Lock()
	for k, v := range repo.Services {
		if v.ServiceID == id {
			delete(repo.Services, k)
		}
	}
	repo.Unlock()
}

// totalServices returns the number of services registered in the repo.
func (repo *ServiceRepo) totalServices() int {
	repo.RLock()
	var size = len(repo.Services)
	repo.RUnlock()
	return size
}

// getServiceIDs returns slice of services' IDs in the repo.
func (repo *ServiceRepo) getServiceIDs() []string {
	var serviceIDs = make([]string, 0, repo.totalServices())
	repo.RLock()
	for i := range repo.Services {
		serviceIDs = append(serviceIDs, i)
	}
	repo.RUnlock()
	return serviceIDs
}

// addServicesFromConfig reads info about services from the config file and fulfill the repo.
func (repo *ServiceRepo) addServicesFromConfig(config *Config) {
	// sanity check, but basically should be always passed
	if config.ServicesConnSettings == nil {
		log.Warn("connection settings for service are not defined, do nothing")
		return
	}

	log.Debug("adding system service")
	repo.addService("system:0", model.Service{ServiceID: "system:0", ConnSettings: model.ServiceConnSetting{ServiceType: "system"}})

	for _, cs := range config.ServicesConnSettings {
		config, err := pgx.ParseConfig(cs.Conninfo)
		if err != nil {
			log.Warnf("%s: %s, skip", cs.Conninfo, err)
			continue
		}
		// check connection using passed Conninfo, if successful add it to Service Repo
		db, err := store.NewDBConfig(config)
		if err != nil {
			log.Warnf("%s: %s, skip", cs.Conninfo, err)
			continue
		}
		db.Close() // close connection explicitly to avoid "Connection reset by peer" messages

		s := model.Service{
			ServiceID:    cs.ServiceType + ":" + strconv.Itoa(int(config.Port)),
			ConnSettings: cs,
			Exporter:     nil,
		}

		// Adding "host" into key because user might specify services with the same port (but the are running on different hosts)
		var key = strings.Join([]string{cs.ServiceType, config.Host, strconv.Itoa(int(config.Port))}, ":")
		repo.addService(key, s)
		log.Infof("service [%s] registered successfully", s.ServiceID)
	}
}

// setupServices attaches metrics exporters to the services in the repo.
func (repo *ServiceRepo) setupServices(config *Config) error {
	var servicesIDs = repo.getServiceIDs()

	for _, id := range servicesIDs {
		var service = repo.getService(id)
		if service.Exporter == nil {
			service.ProjectID = config.ProjectID

			// create exporter for the service
			exporter, err := newExporter(service, repo)
			if err != nil {
				return err
			}
			service.Exporter = exporter

			// running in PULL mode, the exporter should be registered. In PUSH mode this is done during the push.
			if config.RuntimeMode == runtimeModePull {
				prometheus.MustRegister(service.Exporter)
				log.Infof("exporter registered for %s", service.ServiceID)
			}

			// put updated service copy into repo
			repo.addService(id, service)
		}
	}

	return nil
}

// startBackgroundDiscovery looking for services and add them to the repo.
func (repo *ServiceRepo) startBackgroundDiscovery(ctx context.Context, config *Config) {
	log.Debug("starting background auto-discovery")

	// add pseudo-service for system metrics
	log.Debug("adding system service")
	repo.addService("system:0", model.Service{ServiceID: "system:0", ConnSettings: model.ServiceConnSetting{ServiceType: "system"}})

	for {
		if err := repo.lookupServices(config); err != nil {
			log.Warnln("auto-discovery: lookup failed, skip; ", err)
			continue
		}
		if err := repo.setupServices(config); err != nil {
			log.Warnln("auto-discovery: create exporter failed, skip; ", err)
			continue
		}

		// sleep until timeout or exit
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
func (repo *ServiceRepo) lookupServices(config *Config) error {
	pids, err := process.Pids()
	if err != nil {
		return err
	}

	// walk through the pid list and looking for the processes with appropriate names
	for _, pid := range pids {
		proc, err := process.NewProcess(pid)
		if err != nil {
			log.Debugf("auto-discovery: failed to create process struct for pid %d: %s; skip", pid, err)
			continue
		}

		name, err := proc.Name()
		if err != nil {
			log.Warnf("auto-discovery: no process name for pid %d: %s; skip", pid, err)
			continue // skip processes with no names
		}

		switch name {
		case "postgres":
			ppid, _ := proc.Ppid() // error doesn't matter here, even if ppid will be 0 - we're interested in ppid == 1
			if ppid == 1 {
				postgres, err := discoverPostgres(proc, config)
				if err != nil {
					log.Warnf("auto-discovery: postgres service discovery failed: %s; skip", err)
					break
				}

				// check service in the repo
				if s := repo.getService(postgres.ServiceID); s.ServiceID == postgres.ServiceID {
					log.Debugf("auto-discovery: postgres service [%s] already in the repository, skip", s.ServiceID)
					break
				}

				repo.addService(postgres.ServiceID, postgres) // add postgresql service to the repo
				log.Infof("auto-discovery: discovered new postgres service [%s]", postgres.ServiceID)
			}
		case "pgbouncer":
			pgbouncer, err := discoverPgbouncer(proc, config)
			if err != nil {
				log.Warnf("auto-discovery: pgbouncer service discovery failed: %s; skip", err)
				break
			}

			// check service in the repo
			if s := repo.getService(pgbouncer.ServiceID); s.ServiceID == pgbouncer.ServiceID {
				log.Debugf("auto-discovery: pgbouncer service [%s] already in the repository, skip", s.ServiceID)
				break
			}

			repo.addService(pgbouncer.ServiceID, pgbouncer) // add pgbouncer service to the repo
			log.Infof("auto-discovery: discovered new pgbouncer service [%s]", pgbouncer.ServiceID)
		default:
			continue // others are not interesting
		}
	}
	return nil
}

// discoverPostgres reads "datadir" argument from Postmaster's cmdline string and reads postmaster.pid stored in data
// directory. Using postmaster.pid data construct "conninfo" string and test it through making a connection.
func discoverPostgres(proc *process.Process, appConfig *Config) (model.Service, error) {
	cmdline, err := proc.CmdlineSlice()
	if err != nil {
		return model.Service{}, err
	}
	// parse cmdline
	datadirCmdPath, err := parsePostgresProcessCmdline(cmdline)
	if err != nil {
		return model.Service{}, err
	}

	connParams, err := newPostgresConnectionParams(datadirCmdPath + "/postmaster.pid")
	if err != nil {
		return model.Service{}, err
	}

	// Construct the connection string using the data from postmaster.pid and user-defined defaults.
	// Depending on configured Postgres there can be UNIX-based or TCP-based connection string
	var connString string
	for _, v := range []bool{true, false} {
		connString = newPostgresConnectionString(connParams, appConfig.Defaults, v)
		if err := attemptConnect(connString); err == nil {
			// no need to continue because connection with created connString was successful
			break
		}
	}

	s := model.Service{
		ServiceID:    model.ServiceTypePostgresql + ":" + strconv.Itoa(connParams.listenPort),
		ProjectID:    appConfig.ProjectID,
		ConnSettings: model.ServiceConnSetting{ServiceType: model.ServiceTypePostgresql, Conninfo: connString},
		Exporter:     nil,
	}

	log.Debugf("auto-discovery: postgres service has been found, pid %d, available through %s:%d", proc.Pid, connParams.listenAddr, connParams.listenPort)
	return s, nil
}

// parsePostgresProcessCmdline parses postgres process cmdline for data directory argument
func parsePostgresProcessCmdline(cmdline []string) (string, error) {
	for i, arg := range cmdline {
		if arg == "-D" && len(cmdline) > i+1 {
			return cmdline[i+1], nil
		}
	}
	return "", fmt.Errorf("data directory argument not found")
}

// newPostgresConnectionParams reads connection parameters from postmaster.pid
func newPostgresConnectionParams(pidFilePath string) (connectionParams, error) {
	p := connectionParams{}
	content, err := ioutil.ReadFile(pidFilePath)
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
func discoverPgbouncer(proc *process.Process, appConfig *Config) (model.Service, error) {
	log.Debug("looking for pgbouncer services")

	cmdline, err := proc.CmdlineSlice()
	if err != nil {
		return model.Service{}, err
	}

	// inifile is always the last argument in cmdline string, take it
	var iniFilePath = cmdline[len(cmdline)-1]

	// parse ini file
	connParams, err := parsePgbouncerIniFile(iniFilePath)
	if err != nil {
		return model.Service{}, err
	}

	connString := newPgbouncerConnectionString(connParams, appConfig.Defaults)

	if err := attemptConnect(connString); err != nil {
		return model.Service{}, err
	}

	s := model.Service{
		ServiceID:    model.ServiceTypePgbouncer + ":" + strconv.Itoa(connParams.listenPort),
		ProjectID:    appConfig.ProjectID,
		ConnSettings: model.ServiceConnSetting{ServiceType: model.ServiceTypePgbouncer, Conninfo: connString},
		Exporter:     nil,
	}

	log.Debugf("auto-discovery: pgbouncer service has been found, pid %d, available through %s:%d", proc.Pid, connParams.listenAddr, connParams.listenPort)
	return s, nil
}

// parsePgbouncerIniFile reads pgbouncer's config ini file and returns connection parameters required for constructing connection string.
func parsePgbouncerIniFile(iniFilePath string) (connectionParams, error) {
	// read the content of inifile
	file, err := os.Open(iniFilePath)
	if err != nil {
		return connectionParams{}, err
	}
	defer func() { _ = file.Close() }()

	var paramName, paramValue string
	var connParams connectionParams

	log.Debugf("auto-discovery: start reading %s", iniFilePath)

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
			log.Debugf("no value parameter %s; skip", line)
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
	db, err := store.NewDB(connString)
	if err != nil {
		return err
	}
	db.Close()
	return nil
}
