#!/bin/sh
# Runs once, on the node that creates the cluster. Patroni passes a superuser connection string as $1.
#   app  owns the schema (used only for migrations and the audit)
#   svc  is what the services log in as: read / insert / update, no DDL (least privilege). DELETE only on outbox and
#        notifications, for their retention jobs; control's bootstrap grants it once the tables exist.
set -e
psql "$1" -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE app LOGIN PASSWORD 'apppass';
CREATE ROLE svc LOGIN PASSWORD 'svcpass';
CREATE DATABASE trading OWNER app;
SQL
psql "$1" -d trading -v ON_ERROR_STOP=1 <<'SQL'
GRANT CONNECT ON DATABASE trading TO svc;
GRANT USAGE ON SCHEMA public TO svc;
ALTER DEFAULT PRIVILEGES FOR ROLE app IN SCHEMA public GRANT SELECT, INSERT, UPDATE ON TABLES TO svc;
ALTER DEFAULT PRIVILEGES FOR ROLE app IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO svc;
SQL
