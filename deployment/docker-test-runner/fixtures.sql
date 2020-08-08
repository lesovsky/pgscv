-- schema fixtures
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
SELECT pg_stat_statements_reset();

CREATE ROLE pgscv WITH LOGIN SUPERUSER;

CREATE DATABASE pgscv_fixtures OWNER pgscv;
\c pgscv_fixtures pgscv

-- create table with invalid index
CREATE TABLE orders (id SERIAL PRIMARY KEY, name TEXT, status INT);
CREATE INDEX orders_status_idx ON orders (status);
UPDATE pg_index SET indisvalid = false WHERE indexrelid = (SELECT oid FROM pg_class WHERE relname = 'orders_status_idx');

-- create table with redundant index
CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT, size INTEGER, weight INTEGER, color INTEGER);
CREATE INDEX products_name_idx ON products (name);
CREATE INDEX products_name_size_idx ON products (name, size);

-- create table with near-to-overflow sequence
CREATE TABLE events (id SERIAL PRIMARY KEY, key TEXT, payload TEXT);
SELECT setval('events_id_seq', 2000000000);

-- create tables with non-indexed foreign key
CREATE TABLE accounts (id SERIAL PRIMARY KEY, name TEXT, email TEXT, passowrd TEXT, status INTEGER);
CREATE TABLE statuses (id SERIAL PRIMARY KEY, name TEXT);
ALTER TABLE accounts ADD CONSTRAINT accounts_status_constraint FOREIGN KEY (status) REFERENCES statuses (id);

-- create tables with foreign key and different columns types
CREATE TABLE persons (id SERIAL PRIMARY KEY, name TEXT, email TEXT, passowrd TEXT, property BIGINT);
CREATE TABLE properties (id SERIAL PRIMARY KEY, name TEXT);
ALTER TABLE persons ADD CONSTRAINT persons_properties_constraint FOREIGN KEY (property) REFERENCES properties (id);

-- create table with no primary/unique key
CREATE TABLE migrations (id INT, created_at TIMESTAMP, description TEXT);
CREATE INDEX migrations_created_at_idx ON migrations (created_at);
