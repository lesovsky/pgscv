#!/bin/bash

MAIN_DATADIR=/var/lib/postgresql/data/main
STDB_DATADIR=/var/lib/postgresql/data/standby

# init postgres
su - postgres -c "/usr/lib/postgresql/14/bin/initdb -k -E UTF8 --locale=en_US.UTF-8 -D ${MAIN_DATADIR}"

# add extra config parameters
cat >> ${MAIN_DATADIR}/postgresql.auto.conf <<EOF
ssl = on
ssl_cert_file = '/etc/ssl/certs/ssl-cert-snakeoil.pem'
ssl_key_file = '/etc/ssl/private/ssl-cert-snakeoil.key'
logging_collector = on
log_directory = '/var/log/postgresql'
track_io_timing = on
track_functions = all
shared_preload_libraries = 'pg_stat_statements'
EOF

echo "host all pgscv 127.0.0.1/32 trust" >> ${MAIN_DATADIR}/pg_hba.conf

# run main postgres
su - postgres -c "/usr/lib/postgresql/14/bin/pg_ctl -w -t 30 -l /var/run/postgresql/startup-main.log -D ${MAIN_DATADIR} start"
su - postgres -c "psql -c \"SELECT pg_create_physical_replication_slot('standby_test_slot')\""

# run standby postgres
su - postgres -c "pg_basebackup -P -R -X stream -c fast -h 127.0.0.1 -p 5432 -U postgres -D ${STDB_DATADIR}"
cat >> ${STDB_DATADIR}/postgresql.auto.conf <<EOF
port = 5433
primary_slot_name = 'standby_test_slot'
log_filename = 'postgresql-standby.log'
EOF
su - postgres -c "/usr/lib/postgresql/14/bin/pg_ctl -w -t 30 -l /var/run/postgresql/startup-standby.log -D ${STDB_DATADIR} start"

# add fixtures, tiny workload
su - postgres -c 'psql -f /usr/local/testing/fixtures.sql'
su - postgres -c "pgbench -i -s 5 pgscv_fixtures"
su - postgres -c "pgbench -T 5 pgscv_fixtures"

# configure pgbouncer
sed -i -e 's/^;\* = host=testserver$/* = host=127.0.0.1/g' /etc/pgbouncer/pgbouncer.ini
sed -i -e 's/^;admin_users = .*$/admin_users = pgscv/g' /etc/pgbouncer/pgbouncer.ini
sed -i -e 's/^;pool_mode = session$/pool_mode = transaction/g' /etc/pgbouncer/pgbouncer.ini
sed -i -e 's/^;ignore_startup_parameters = .*$/ignore_startup_parameters = extra_float_digits/g' /etc/pgbouncer/pgbouncer.ini
echo '"pgscv" ""' > /etc/pgbouncer/userlist.txt

# run pgbouncer
su - postgres -c "/usr/sbin/pgbouncer -d /etc/pgbouncer/pgbouncer.ini"

# check services availability
pg_isready -t 10 -h 127.0.0.1 -p 5432 -U pgscv -d postgres
pg_isready -t 10 -h 127.0.0.1 -p 5433 -U pgscv -d postgres
pg_isready -t 10 -h 127.0.0.1 -p 6432 -U pgscv -d pgbouncer
