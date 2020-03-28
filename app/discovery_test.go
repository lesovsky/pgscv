package app

import (
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"os"
	"pgscv/app/model"
	"testing"
)

func TestServiceRepo_startInitialDiscovery(t *testing.T) {
	var c = &Config{Logger: zerolog.Logger{}}
	repo := NewServiceRepo(c)
	assert.NotNil(t, repo)

	assert.NoError(t, repo.discoverServicesOnce())
	assert.Greater(t, len(repo.Services), 0)

	for i := range repo.Services {
		repo.removeService(repo.Services[i].Pid)
	}
}

// Test_lookupServices проверяет что найдены хоть какие-то сервисы помимо системного
func Test_lookupServices(t *testing.T) {
	if uid := os.Geteuid(); uid != 0 {
		t.Skipf("root privileges required, skip")
	}

	var c = &Config{Logger: zerolog.Logger{}}
	repo := NewServiceRepo(c)
	assert.NotNil(t, repo)

	assert.NoError(t, repo.lookupServices())
	assert.Greater(t, len(repo.Services), 1)
	for _, s := range repo.Services {
		assert.NotEmpty(t, s.ServiceType)
	}
}

// Test_setupServices должен на вход принять пустой сервис и на выходе получить сервис с заполненым ServiceID и Exporter
func Test_setupServices(t *testing.T) {
	var c = &Config{Logger: zerolog.Logger{}}
	repo := NewServiceRepo(c)
	assert.NotNil(t, repo)

	var testCases = []struct {
		service           model.Service
		expectedServiceID string
	}{
		{
			service: model.Service{
				ServiceType: model.ServiceTypePostgresql,
				Pid:         0,
				Host:        "example.org",
				Port:        5432,
				User:        "postgres",
				Password:    "testpass",
				Dbname:      "postgres",
			},
			expectedServiceID: "postgres:5432",
		},
		{
			service: model.Service{
				ServiceType: model.ServiceTypePgbouncer,
				Pid:         0,
				Host:        "example.org",
				Port:        6432,
				User:        "pgbouncer",
				Password:    "testpass",
				Dbname:      "pgbouncer",
			},
			expectedServiceID: "pgbouncer:6432",
		},
		{
			service: model.Service{
				ServiceType: model.ServiceTypeSystem,
			},
			expectedServiceID: "system",
		},
	}

	for _, tc := range testCases {
		repo.Services[0] = tc.service
		assert.Empty(t, repo.Services[0].ServiceID)
		assert.Nil(t, repo.Services[0].Exporter)

		assert.NoError(t, repo.setupServices())
		assert.Equal(t, tc.expectedServiceID, repo.Services[0].ServiceID)
		assert.NotNil(t, repo.Services[0].Exporter)

		repo.removeService(repo.Services[0].Pid)
	}
}
