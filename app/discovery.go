package app

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/process"
	"io"
	"io/ioutil"
	"pgscv/app/log"
	"pgscv/app/model"
	"strconv"
	"strings"
	"time"
)

// ServiceRepo is the store of discovered services
type ServiceRepo struct {
	Services map[int32]model.Service
	Config   *Config
}

// NewServiceRepo creates new services repository
func NewServiceRepo(config *Config) *ServiceRepo {
	return &ServiceRepo{
		Services: make(map[int32]model.Service),
		Config:   config,
	}
}

// discoverServiceOnce performs initial service discovery required at application startup
func (repo *ServiceRepo) discoverServicesOnce() error {
	log.Debug("starting initial discovery")

	// add pseudo-service for system metrics
	log.Debug("adding system service")
	repo.Services[0] = model.Service{ServiceType: model.ServiceTypeSystem}

	// search services and add them to the repo
	if err := repo.lookupServices(); err != nil {
		return err
	}

	// configure exporters for found services
	if err := repo.setupServices(); err != nil {
		return err
	}

	log.Debugf("finish initial discovery: found %d services", len(repo.Services))
	return nil
}

// startBackgroundDiscovery is periodically searches new services
func (repo *ServiceRepo) startBackgroundDiscovery(ctx context.Context) {
	log.Debug("starting background auto-discovery")

	for {
		select {
		case <-time.After(60 * time.Second):
			// remove services that become unavailable during last discovery interval
			if removed := repo.removeStaleServices(); removed > 0 {
				log.Infof("auto-discovery: removed %d stale services", removed)
			}

			if err := repo.lookupServices(); err != nil {
				log.Warnln("auto-discovery: lookup failed, skip; ", err)
				continue
			}
			if err := repo.setupServices(); err != nil {
				log.Warnln("auto-discovery: create exporter failed, skip; ", err)
				continue
			}
		case <-ctx.Done():
			log.Info("exit signaled, stop auto-discovery")
			return
		}
	}
}

// removeStaleServices checks services availability (is associated process live and it's the same) and remove unavailable
func (repo *ServiceRepo) removeStaleServices() (removed int) {
	for pid, service := range repo.Services {
		if !service.IsAvailable() {
			repo.removeService(pid)
			removed++
		}
	}
	return removed
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
			log.Debugf("auto-discovery: service with pid %d already in the repository, skip", pid)
			continue // skip processes when services with such pids already in the service repo
		}

		proc, err := process.NewProcess(pid)
		if err != nil {
			log.Debugf("auto-discovery: failed to create process struct for pid %d, skip; %s", pid, err)
			continue
		}

		name, err := proc.Name()
		if err != nil {
			log.Warnf("auto-discovery: no process name for pid %d, skip; %s", pid, err)
			continue // skip processes with no names
		}

		switch name {
		case "postgres":
			ppid, _ := proc.Ppid() // error doesn't matter here, even if ppid will be 0 - we're interested in ppid == 1
			if ppid == 1 {
				ctime, err := proc.CreateTime()
				if err != nil {
					log.Warnln("get process create time failed: ", err)
					break
				}
				postgres, err := discoverPostgres(proc)
				if err != nil {
					log.Warnln("postgresql service has been found, but skipped due to: ", err)
					break
				}
				postgres.Validate()
				postgres.ProcessName = name
				postgres.ProcessCreateTime = ctime
				postgres.Password = repo.Config.DefaultCredentials.PostgresPassword
				repo.Services[pid] = postgres // add postgresql service to the repo
			}
		case "pgbouncer":
			ctime, err := proc.CreateTime()
			if err != nil {
				log.Warnln("get process create time failed: ", err)
				break
			}
			pgbouncer, err := discoverPgbouncer(proc)
			if err != nil {
				log.Warnln("pgbouncer service has been found, but skipped due to: ", err)
				break
			}
			pgbouncer.Validate()
			pgbouncer.ProcessName = name
			pgbouncer.ProcessCreateTime = ctime
			pgbouncer.Password = repo.Config.DefaultCredentials.PgbouncerPassword
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
				newService.ServiceID = "system"
			}

			// create exporter for the service
			exporter, err := newExporter(newService, repo)
			if err != nil {
				return err
			}
			newService.Exporter = exporter

			// для PULL режима надо зарегать новоявленного экспортера, для PUSH это сделается в процессе самого пуша
			if repo.Config.RuntimeMode == runtimeModePull {
				prometheus.MustRegister(newService.Exporter)
				log.Infof("auto-discovery: exporter registered for %s with pid %d", newService.ServiceID, newService.Pid)
			}

			// put updated service copy into repo
			repo.Services[i] = newService
		}
	}
	return nil
}

// RemoveService removes service from the list (in case of its unavailability)
func (repo *ServiceRepo) removeService(pid int32) {
	prometheus.Unregister(repo.Services[pid].Exporter)
	log.Infof("auto-discovery: collector unregistered for %s, pid %d", repo.Services[pid].ServiceID, pid)
	delete(repo.Services, pid)
}

// discoverPgbouncer
func discoverPgbouncer(proc *process.Process) (model.Service, error) {
	log.Debug("looking for pgbouncer services")

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

	log.Debugf("auto-discovery: start reading %s", conffile)
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
					laddr = model.DefaultServiceHost
				}
			case "listen_port":
				lport, err = strconv.Atoi(strings.Trim(pvalue, ""))
				if err != nil {
					log.Logger.Info().Err(err).Msgf("failed convert listen_port value from string to integer")
					lport = model.DefaultPgbouncerPort
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

	// TODO: laddr не используется, то есть мы его типа ищем, но в конечном счете он не используется дальше (для коннекта используется unix_socket_dir

	log.Infof("auto-discovery: pgbouncer service has been found, pid %d, available through %s, port %d", proc.Pid, sdir, 6432)
	return model.Service{ServiceType: model.ServiceTypePgbouncer, Pid: proc.Pid, Host: sdir, Port: uint16(lport)}, nil
}

// discoverPostgres
// Postgres discovering works through looking for Postgres's UNIX socket. Potentially Postgres might be configured
// without listening on UNIX socket, thus agent should foresee additional methods for such case.
func discoverPostgres(proc *process.Process) (model.Service, error) {
	log.Debug("looking for postgresql services")
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

	log.Infof("auto-discovery: postgresql service has been found, pid %d, available through %s, port %d", proc.Pid, unixsocketdir, port)
	return model.Service{ServiceType: model.ServiceTypePostgresql, Pid: proc.Pid, Host: unixsocketdir, Port: uint16(port)}, nil
}
