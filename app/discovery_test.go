package app

import (
	"fmt"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"os"
	"pgscv/app/model"
	"testing"
)

func TestServiceRepo_startInitialDiscovery(t *testing.T) {
	var c = &Config{
		Logger:           zerolog.Logger{},
		DiscoveryEnabled: true,
		Credentials: Credentials{
			PostgresUser:  "postgres",
			PgbouncerUser: "pgbouncer",
		},
	}

	repo := NewServiceRepo(c)
	assert.NotNil(t, repo)

	assert.NoError(t, repo.createServicesFromDiscovery())
	assert.Greater(t, len(repo.Services), 0)

	for i := range repo.Services {
		repo.removeService(repo.Services[i].Pid)
	}
}

// Задача теста проверить что на каждый валидный URL создается сервис -- что там внутри сервиса в этом тесте НЕ важно
func TestServiceRepo_configureServicesWithURL(t *testing.T) {
	var c = &Config{
		Logger:           zerolog.Logger{},
		DiscoveryEnabled: true,
		URLStrings:       nil,
		Credentials: Credentials{
			PostgresUser:  "postgres",
			PgbouncerUser: "pgbouncer",
		},
	}
	var testCases = []struct {
		name       string
		urlStrings []string
		want       int
	}{
		{
			name: "valid postgres urls",
			urlStrings: []string{
				"postgres://example.org",
				"postgres://postgres@example.org:5433",
				"postgres://postgres:password@example.org:5434",
				"postgres://postgres:password@example.org:5435",
				"postgres://postgres:password@example.org:5436/testdatabase",
			},
			want: 5,
		},
		{
			name: "valid pgbouncer urls",
			urlStrings: []string{
				"pgbouncer://example.org",
				"pgbouncer://postgres@example.org:6433",
				"pgbouncer://postgres:password@example.org:6434",
				"pgbouncer://postgres:password@example.org:6435",
				"pgbouncer://postgres:password@example.org:6436/testdatabase",
			},
			want: 5,
		},
		{
			name: "valid and invalid urls",
			urlStrings: []string{
				"pgbouncer://postgres:password@example.org:5432/testdatabase",
				"invalid://postgres:password@example.org:5432/testdatabase",
				"invalid",
			},
			want: 1,
		},
	}

	for _, tc := range testCases {
		fmt.Println(tc.name)
		c.URLStrings = tc.urlStrings
		repo := NewServiceRepo(c)
		assert.NotNil(t, repo)

		err := repo.createServicesFromURL()
		assert.NoError(t, err)
		assert.NotNil(t, repo.Services)
		assert.Equal(t, len(repo.Services), tc.want)

		for i := range repo.Services {
			repo.removeService(repo.Services[i].Pid)
		}
	}
}

// Test_lookupServices проверяет что найдены хоть какие-то сервисы помимо системного
func Test_lookupServices(t *testing.T) {
	if uid := os.Geteuid(); uid != 0 {
		t.Skipf("root privileges required, skip")
	}

	var c = &Config{
		Logger:           zerolog.Logger{},
		DiscoveryEnabled: true,
	}
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
	var c = &Config{
		Logger:           zerolog.Logger{},
		DiscoveryEnabled: true,
		URLStrings:       nil,
		Credentials:      Credentials{PostgresUser: "postgres", PgbouncerUser: "pgbouncer"},
	}
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
