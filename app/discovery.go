//
package app

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/shirou/gopsutil/process"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"time"
)

// Instance is the container for discovered service
type Instance struct {
	Pid          int32 // process identifier
	InstanceType int   // "postgres" or "pgbouncer"
	Host         string
	Port         int
	User         string
	Dbname       string
	Exporter     *Exporter
	ServiceId    string // Service identifier -- отличает сервисы запущенные на одном хосте
	ProjectId    string // Project ID -- объединяет метрики одного проекта
}

type InstanceRepo struct {
	Instances map[int32]Instance
	appConfig *Config
}

func NewInstanceRepo(config *Config) *InstanceRepo {
	return &InstanceRepo{
		Instances: make(map[int32]Instance),
		appConfig: config,
	}
}

func (repo *InstanceRepo) StartInitialDiscovery() error {
	// добавляем псевдо-инстанс для системных метрик
	repo.Instances[0] = Instance{InstanceType: stypeSystem, ServiceId: "system"}

	if err := repo.lookupInstances(); err != nil {
		return err
	}
	if err := repo.setupInstances(); err != nil {
		return err
	}
	return nil
}

// discoveryLoop is the main loop aimed to discover services
func (repo *InstanceRepo) StartBackgroundDiscovery() {
	// TODO: нет кейса для выхода
	for {
		select {
		case <-time.After(60 * time.Second):
			if err := repo.lookupInstances(); err != nil {
				log.Warnf("auto-discovery failed: %s, skip", err)
				continue
			}
			if err := repo.setupInstances(); err != nil {
				log.Warnf("auto-discovery failed: create exporter error: %s, skip", err)
				continue
			}
		}
	}
}

// lookupInstances scans PIDs and looking for required services
func (repo *InstanceRepo) lookupInstances() error {
	allPids, err := process.Pids()
	if err != nil {
		return err
	}

	// проходимся по всем пидам и смотрим что у них за имена, и далее отталкиваеимся от имен и cmdline
	for _, pid := range allPids {
		// если инстанс уже есть в мапе, то пропускаем его
		if _, ok := repo.Instances[pid]; ok {
			log.Debugf("auto-discovery: service with pid %d already in the map, skip", pid)
			continue
		}

		proc, err := process.NewProcess(pid)
		if err != nil {
			log.Debugf("auto-discovery: failed to create process instance for pid %d: %s... skip", pid, err)
			continue
		}

		name, err := proc.Name()
		if err != nil {
			log.Warnf("auto-discovery: failed to obtain process name for pid %d: %s... skip", pid, err)
			continue // пропускаем пиды с пустым именем
		}

		switch name {
		case "postgres":
			ppid, _ := proc.Ppid() // ошибка не имеет значение, даже если ppid в итоге будет равен 0, т.к. сравниваемся с 1-й
			if ppid == 1 {
				pginfo, err := discoverPostgres(proc)
				if err != nil {
					return err
				}
				repo.Instances[pid] = pginfo // добавляем параметры подключения в карту
			}
		case "pgbouncer":
			pgbinfo, err := discoverPgbouncer(proc)
			if err != nil {
				return err
			}
			repo.Instances[pid] = pgbinfo // добавляем параметры подключения в карту
		default:
			continue // остальное нас не интересует
		}
	}

	return nil
}

// setupInstances configures discovered service and adds into the service's list
func (repo *InstanceRepo) setupInstances() error {
	for i, instance := range repo.Instances {
		if instance.Exporter == nil {
			var newInstance = instance
			newInstance.ProjectId = repo.appConfig.ProjectIdStr

			switch instance.InstanceType {
			case stypePostgresql:
				newInstance.ServiceId = "postgres:" + strconv.Itoa(instance.Port)
			case stypePgbouncer:
				newInstance.ServiceId = "pgbouncer:" + strconv.Itoa(instance.Port)
			case stypeSystem:
				// nothing to do
			}

			// создаем экспортер для экземпляра инстанса, затем помещаем созданный экспортер в экземпляр
			var err error
			newInstance.Exporter, err = NewExporter(newInstance, repo)
			if err != nil {
				return err
			}

			// для PULL режима надо зарегать новоявленного экспортера, для PUSH это сделается в процессе самого пуша
			if repo.appConfig.MetricServiceBaseURL == "" {
				prometheus.MustRegister(newInstance.Exporter)
				repo.appConfig.Logger.Info().Msgf("auto-discovery: exporter registered for %s with pid %d", instance.ServiceId, instance.Pid)
			}

			// put update instance copy into repo
			repo.Instances[i] = newInstance
		}
	}
	return nil
}

// removeInstance removes service from the list (in case of its unavailability)
func (repo *InstanceRepo) removeInstance(pid int32) {
	prometheus.Unregister(repo.Instances[pid].Exporter)
	log.Infof("auto-discovery: collector unregistered for %s, process %d", repo.Instances[pid].ServiceId, pid)
	delete(repo.Instances, pid)
}

// discoverPgbouncer
func discoverPgbouncer(proc *process.Process) (Instance, error) {
	// пока тупо возвращаем значение без всякого дискавери
	// надо взять конфиг из cmdline прочитать его и найти параметры порта и адреса
	cmdline, err := proc.CmdlineSlice()
	if err != nil {
		return Instance{}, fmt.Errorf("failed to read pgbouncer's process cmdline: %s", err)
	}
	var conffile = cmdline[len(cmdline)-1]
	content, err := ioutil.ReadFile(conffile)
	if err != nil {
		return Instance{}, err
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
		return Instance{}, fmt.Errorf("pgbouncer's address or port lookup failed")
	}

	log.Infof("auto-discovery: pgbouncer service has been found, pid %d, available through %s, port %d", proc.Pid, sdir, 6432)
	return Instance{InstanceType: stypePgbouncer, Pid: proc.Pid, Host: sdir, Port: lport, User: "pgbouncer", Dbname: "pgbouncer"}, nil
}

// discoverPostgres
func discoverPostgres(proc *process.Process) (Instance, error) {
	// надо найти сокет для коннекта
	cmdline, err := proc.CmdlineSlice()
	if err != nil {
		return Instance{}, fmt.Errorf("failed to read postgres's process cmdline: %s", err)
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
				return Instance{}, fmt.Errorf("failed to read postmaster.pid: %s", err)
			}

			reader := bufio.NewReader(bytes.NewBuffer(content))
			for i := 0; ; i++ {
				line, _, err := reader.ReadLine()
				if err == io.EOF {
					break
				} else if err != nil {
					return Instance{}, fmt.Errorf("failed to content of postmaster.pid: %s", err)
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
	log.Infof("auto-discovery: postgresql service has been found, pid %d, available through %s, port %d", proc.Pid, unixsocketdir, port)
	return Instance{InstanceType: stypePostgresql, Pid: proc.Pid, Host: unixsocketdir, Port: port}, nil
}
