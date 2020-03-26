package app

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/shirou/gopsutil/process"
	"io"
	"io/ioutil"
	"pgscv/app/model"
	"strconv"
	"strings"
	"time"
)

// ServiceRepo is the store of discovered services
type ServiceRepo struct {
	Logger   zerolog.Logger
	Services map[int32]model.Service
	Config   *Config
}

// NewServiceRepo creates new services repository
func NewServiceRepo(config *Config) *ServiceRepo {
	return &ServiceRepo{
		Logger:   config.Logger.With().Str("service", "discovery").Logger(),
		Services: make(map[int32]model.Service),
		Config:   config,
	}
}

// Configure performs initial service discovery using auto-discovery or service URLs provided by user
func (repo *ServiceRepo) Configure(config *Config) error {
	if config.DiscoveryEnabled {
		if err := repo.StartInitialDiscovery(); err != nil {
			return err
		}

		// TODO: что если там произойдет ошибка? по идее нужно делать ретрай
		go repo.StartBackgroundDiscovery()
	} else {
		if err := repo.ConfigureServices(); err != nil {
			return err
		}
	}
	return nil
}

// StartInitialDiscovery performs initial service discovery required at application startup
func (repo *ServiceRepo) StartInitialDiscovery() error {
	repo.Logger.Debug().Msg("starting initial discovery")

	// add pseudo-service for system metrics
	repo.Logger.Debug().Msg("adding system service")
	repo.Services[0] = model.Service{ServiceType: model.ServiceTypeSystem, ServiceID: "system"}

	// search services and add them to the repo
	if err := repo.lookupServices(); err != nil {
		return err
	}

	// configure exporters for found services
	if err := repo.setupServices(); err != nil {
		return err
	}

	repo.Logger.Debug().Msgf("finish initial discovery: found %d services", len(repo.Services))
	return nil
}

// StartBackgroundDiscovery is periodically searches new services
func (repo *ServiceRepo) StartBackgroundDiscovery() {
	repo.Logger.Debug().Msg("starting background discovery")
	// TODO: нет кейса для выхода
	for {
		<-time.After(60 * time.Second)
		if err := repo.lookupServices(); err != nil {
			repo.Logger.Warn().Err(err).Msg("auto-discovery: lookup failed, skip")
			continue
		}
		if err := repo.setupServices(); err != nil {
			repo.Logger.Warn().Err(err).Msg("auto-discovery: create exporter failed, skip")
			continue
		}
	}
}

// ConfigureServices creates service for each specified DSN
func (repo *ServiceRepo) ConfigureServices() error {
	repo.Logger.Debug().Msg("starting initialization using provided URL strings")

	for i, url := range repo.Config.URLStrings {
		var fields = strings.Split(url, "://")
		if fields == nil {
			repo.Logger.Warn().Msgf("schema delimiter not found in URL: %s, skip", url)
			continue
		}

		var serviceType int
		switch fields[0] {
		case "postgres":
			serviceType = model.ServiceTypePostgresql
		case "pgbouncer":
			serviceType = model.ServiceTypePgbouncer
			url = strings.Replace(url, "pgbouncer://", "postgres://", 1) // replace 'pgbouncer://' because 'pgxpool' doesn't understand such prefix
		default:
			repo.Logger.Warn().Msgf("unknown schema in URL: %s, skip", url)
			continue
		}

		config, err := pgxpool.ParseConfig(url)
		if err != nil {
			repo.Logger.Warn().Err(err).Msg("failed to parse URL, skip")
			continue
		}

		var service = model.Service{
			ServiceType: serviceType,
			Pid:         int32(i),
			Host:        config.ConnConfig.Host,
			Port:        config.ConnConfig.Port,
			User:        config.ConnConfig.User,
			Password:    config.ConnConfig.Password,
			Dbname:      config.ConnConfig.Database,
		}

		repo.Services[int32(i)] = service
	}

	// configure exporters for initialised services
	if err := repo.setupServices(); err != nil {
		return err
	}

	repo.Logger.Debug().Msgf("finish initialisation: setting up %d services", len(repo.Services))
	return nil
}

// lookupServices scans PIDs and looking for required services
// Current agent implementation searches services using local procfs filesystem
func (repo *ServiceRepo) lookupServices() error {
	pids, err := process.Pids()
	if err != nil {
		return err
	}

	// проходимся по всем пидам и смотрим что у них за имена, и далее отталкиваеимся от имен и cmdline
	for _, pid := range pids {
		if _, ok := repo.Services[pid]; ok {
			repo.Logger.Debug().Msgf("auto-discovery: service with pid %d already in the repository, skip", pid)
			continue // skip processes when services with such pids already in the service repo
		}

		proc, err := process.NewProcess(pid)
		if err != nil {
			repo.Logger.Debug().Err(err).Msgf("auto-discovery: failed to create process struct for pid %d, skip", pid)
			continue
		}

		name, err := proc.Name()
		if err != nil {
			repo.Logger.Warn().Err(err).Msgf("auto-discovery: no process name for pid %d, skip", pid)
			continue // skip processes with no names
		}

		switch name {
		case "postgres":
			ppid, _ := proc.Ppid() // error doesn't matter here, even if ppid will be 0 - we're interested in ppid == 1
			if ppid == 1 {
				postgres, err := discoverPostgres(proc)
				if err != nil {
					repo.Logger.Warn().Err(err).Msg("postgresql service has been found, but skipped due to:")
					break
				}
				postgres.User = repo.Config.Credentials.PostgresUser
				postgres.Password = repo.Config.Credentials.PostgresPass
				repo.Services[pid] = postgres // add postgresql service to the repo
			}
		case "pgbouncer":
			pgbouncer, err := discoverPgbouncer(proc)
			if err != nil {
				repo.Logger.Warn().Err(err).Msg("pgbouncer service has been found, but skipped due to:")
				break
			}
			pgbouncer.User = repo.Config.Credentials.PgbouncerUser
			pgbouncer.Password = repo.Config.Credentials.PgbouncerPass
			repo.Services[pid] = pgbouncer // add pgbouncer service to the repo
		default:
			continue // others are not interesting
		}
	}
	return nil
}

// setupServices configures discovered service and adds into the service's list
func (repo *ServiceRepo) setupServices() error {
	for i, service := range repo.Services {
		if service.Exporter == nil {
			var newService = service
			newService.ProjectID = repo.Config.ProjectIDStr

			switch service.ServiceType {
			case model.ServiceTypePostgresql:
				newService.ServiceID = "postgres:" + strconv.Itoa(int(service.Port))
			case model.ServiceTypePgbouncer:
				newService.ServiceID = "pgbouncer:" + strconv.Itoa(int(service.Port))
			case model.ServiceTypeSystem:
				// nothing to do
			}

			// create exporter for the service
			exporter, err := newExporter(newService, repo)
			if err != nil {
				return err
			}
			newService.Exporter = exporter

			// для PULL режима надо зарегать новоявленного экспортера, для PUSH это сделается в процессе самого пуша
			if repo.Config.MetricServiceBaseURL == "" {
				prometheus.MustRegister(newService.Exporter)
				repo.Logger.Info().Msgf("auto-discovery: exporter registered for %s with pid %d", newService.ServiceID, newService.Pid)
			}

			// put updated service copy into repo
			repo.Services[i] = newService
		}
	}
	return nil
}

// RemoveService removes service from the list (in case of its unavailability)
func (repo *ServiceRepo) RemoveService(pid int32) {
	//prometheus.Unregister(repo.Services[pid].Exporter)
	repo.Logger.Info().Msgf("auto-discovery: collector unregistered for %s, process %d", repo.Services[pid].ServiceID, pid)
	delete(repo.Services, pid)
}

// discoverPgbouncer
func discoverPgbouncer(proc *process.Process) (model.Service, error) {
	log.Debug().Msg("looking for pgbouncer services")

	// пока тупо возвращаем значение без всякого дискавери
	// надо взять конфиг из cmdline прочитать его и найти параметры порта и адреса
	cmdline, err := proc.CmdlineSlice()
	if err != nil {
		return model.Service{}, fmt.Errorf("failed to read pgbouncer's process cmdline: %s", err)
	}
	var conffile = cmdline[len(cmdline)-1]
	content, err := ioutil.ReadFile(conffile)
	if err != nil {
		return model.Service{}, err
	}
	reader := bufio.NewReader(bytes.NewBuffer(content))

	// довольно интересное поведение баунсера -- к unix-сокету можно подключиться без авторизации, а вот в tcp-порту, нужно уже иметь все необходимые реквизиты
	// пока просто подключаемся к unix сокету
	// TODO: вместо стандартного юзера следует подключаться под спец.юзером -- например pgscv
	var pname, pvalue string
	var sdir, laddr string
	var lport int

	log.Debug().Msgf("auto-discovery: start reading %s", conffile)
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}

		// looking for listen address and port settings, use them as connection settings
		if len(line) > 0 {
			_, err := fmt.Sscanf(string(line), "%s = %s", &pname, &pvalue)
			if err != nil {
				continue
			}
			switch strings.Trim(pname, " ") {
			case "listen_addr":
				laddr = strings.Trim(pvalue, " ")    // remove all spaces
				laddr = strings.Split(laddr, ",")[0] // take first address
				if laddr == "*" {
					laddr = "127.0.0.1"
				}
			case "listen_port":
				lport, err = strconv.Atoi(strings.Trim(pvalue, ""))
				if err != nil {
					lport = 6432
				}
			case "unix_socket_dir":
				sdir = pvalue

			}
		} // end if
	}

	// sanity check
	if lport == 0 || laddr == "" || sdir == "" {
		return model.Service{}, fmt.Errorf("pgbouncer's address or port lookup failed")
	}

	log.Info().Msgf("auto-discovery: pgbouncer service has been found, pid %d, available through %s, port %d", proc.Pid, sdir, 6432)
	return model.Service{ServiceType: model.ServiceTypePgbouncer, Pid: proc.Pid, Host: sdir, Port: uint16(lport), User: "pgbouncer", Dbname: "pgbouncer"}, nil
}

// discoverPostgres
// Postgres discovering works through looking for Postgres's UNIX socket. Potentially Postgres might be configured
// without listening on UNIX socket, thus agent should foresee additional methods for such case.
func discoverPostgres(proc *process.Process) (model.Service, error) {
	log.Debug().Msg("looking for postgresql services")
	// надо найти сокет для коннекта
	cmdline, err := proc.CmdlineSlice()
	if err != nil {
		return model.Service{}, fmt.Errorf("failed to read postgres's process cmdline: %s", err)
	}

	var datadir, unixsocketdir string
	//var listen_addr string
	var port int
	for i := range cmdline {
		if cmdline[i] == "-D" {
			datadir = cmdline[i+1] // теоретически можно выйти за границы массива, но имхо это невозмжно т.к. после -D обязательно должен идти путь

			// прочитать postmaster.pid и выснить сокет к подключению, заодно порт в качестве уникального атрибута
			content, err := ioutil.ReadFile(datadir + "/postmaster.pid")
			if err != nil {
				return model.Service{}, fmt.Errorf("failed to read postmaster.pid: %s", err)
			}

			reader := bufio.NewReader(bytes.NewBuffer(content))
			for i := 0; ; i++ {
				line, _, err := reader.ReadLine()
				if err == io.EOF {
					break
				} else if err != nil {
					return model.Service{}, fmt.Errorf("failed reading content of postmaster.pid: %s", err)
				}
				switch i {
				case 3:
					port, err = strconv.Atoi(string(line))
					if err != nil {
						return model.Service{}, fmt.Errorf("failed reading content of postmaster.pid: %s", err)
					}
				case 4:
					unixsocketdir = string(line)
					//case 5:
					//	listen_addr = string(line)
				}
			}
		}
	}

	log.Info().Msgf("auto-discovery: postgresql service has been found, pid %d, available through %s, port %d", proc.Pid, unixsocketdir, port)
	return model.Service{ServiceType: model.ServiceTypePostgresql, Pid: proc.Pid, Host: unixsocketdir, Port: uint16(port), Dbname: "postgres"}, nil
}
