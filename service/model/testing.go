package model

// TestSystemService returns system service for testing purposes
func TestSystemService() Service {
	return Service{
		ServiceID: "system",
		ProjectID: "1",
		ConnSettings: ServiceConnSetting{
			ServiceType: ServiceTypeSystem,
		},
	}
}

// TestPostgresService returns postgres service for testing purposes
func TestPostgresService() Service {
	return Service{
		ServiceID: "postgres:5432",
		ProjectID: "1",
		ConnSettings: ServiceConnSetting{
			ServiceType: ServiceTypePostgresql,
			Conninfo:    "host=127.0.0.1 port=5432 user=pgscv dbname=postgres",
		},
	}
}

// TestPgbouncerService returns pgbouncer service for testing purposes
func TestPgbouncerService() Service {
	return Service{
		ServiceID: "pgbouncer:6432",
		ProjectID: "1",
		ConnSettings: ServiceConnSetting{
			ServiceType: ServiceTypePgbouncer,
			Conninfo:    "host=127.0.0.1 port=6432 user=pgscv dbname=pgbouncer",
		},
	}
}
