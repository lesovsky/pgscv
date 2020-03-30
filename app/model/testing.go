package model

import (
	"github.com/shirou/gopsutil/process"
	"os"
)

// NewTestService runs 'sleep 10' command and creates test service with pid, name, create time of that sleep command.
// Also returns a teardown function which should be executed in the parent function (e.g. using defer).
func NewTestService() (*Service, func(), error) {
	sleep, err := os.StartProcess("/bin/sleep", []string{"10"}, &os.ProcAttr{})
	if err != nil {
		return nil, nil, err
	}

	proc, err := process.NewProcess(int32(sleep.Pid))
	if err != nil {
		_ = sleep.Kill()
		return nil, nil, err
	}

	name, err := proc.Name()
	if err != nil {
		_ = sleep.Kill()
		return nil, nil, err
	}

	ctime, err := proc.CreateTime()
	if err != nil {
		_ = sleep.Kill()
		return nil, nil, err
	}

	var s = &Service{
		Pid:               proc.Pid,
		ProcessName:       name,
		ProcessCreateTime: ctime,
	}

	return s, func() {
		name, _ = proc.Name()
		if name == "sleep" {
			_ = proc.Kill()
		}
	}, nil
}
