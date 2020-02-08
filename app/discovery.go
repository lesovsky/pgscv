//
package app

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/shirou/gopsutil/process"
	"io"
	"io/ioutil"
	"scout/app/model"
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

// StartInitialDiscovery performs initial service discovery required at application startup
func (repo *ServiceRepo) StartInitialDiscovery() error {
	// add pseudo-service for system metrics
	repo.Services[0] = model.Service{ServiceType: model.ServiceTypeSystem, ServiceId: "system"}

	// search services and add them to the repo
	if err := repo.lookupServices(); err != nil {
		return err
	}

	// configure exporters for found services
	if err := repo.setupServices(); err != nil {
		return err
	}
	return nil
}

// StartBackgroundDiscovery is periodically searches new services
func (repo *ServiceRepo) StartBackgroundDiscovery() {
	// TODO: нет кейса для выхода
	for {
		select {
		case <-time.After(60 * time.Second):
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
					return err
				}
				repo.Services[pid] = postgres // add postgresql service to the repo
			}
		case "pgbouncer":
			pgbouncer, err := discoverPgbouncer(proc)
			if err != nil {
				return err
			}
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
			newService.ProjectId = repo.Config.ProjectIdStr

			switch service.ServiceType {
			case model.ServiceTypePostgresql:
				newService.ServiceId = "postgres:" + strconv.Itoa(service.Port)
			case model.ServiceTypePgbouncer:
				newService.ServiceId = "pgbouncer:" + strconv.Itoa(service.Port)
			case model.ServiceTypeSystem:
				// nothing to do
			}

			// create exporter for the service
			exporter, err := NewExporter(newService, repo)
			if err != nil {
				return err
			}
			newService.Exporter = exporter

			// для PULL режима надо зарегать новоявленного экспортера, для PUSH это сделается в процессе самого пуша
			if repo.Config.MetricServiceBaseURL == "" {
				prometheus.MustRegister(newService.Exporter)
				repo.Logger.Info().Msgf("auto-discovery: exporter registered for %s with pid %d", newService.ServiceId, newService.Pid)
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
	repo.Logger.Info().Msgf("auto-discovery: collector unregistered for %s, process %d", repo.Services[pid].ServiceId, pid)
	delete(repo.Services, pid)
}

// discoverPgbouncer
func discoverPgbouncer(proc *process.Process) (model.Service, error) {
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
	return model.Service{ServiceType: model.ServiceTypePgbouncer, Pid: proc.Pid, Host: sdir, Port: lport, User: "pgbouncer", Dbname: "pgbouncer"}, nil
}

// discoverPostgres
func discoverPostgres(proc *process.Process) (model.Service, error) {
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
					return model.Service{}, fmt.Errorf("failed to content of postmaster.pid: %s", err)
				}
				switch i {
				case 3:
					port, _ = strconv.Atoi(string(line))
				case 4:
					unixsocketdir = string(line)
					//case 5:
					//	listen_addr = string(line)
				}
			}
		}
	}
	log.Info().Msgf("auto-discovery: postgresql service has been found, pid %d, available through %s, port %d", proc.Pid, unixsocketdir, port)
	return model.Service{ServiceType: model.ServiceTypePostgresql, Pid: proc.Pid, Host: unixsocketdir, Port: port}, nil
}
