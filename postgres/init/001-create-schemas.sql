-- PostgreSQL single-database, multi-schema per service (§ schema separation)
-- metric-service owns: users, metrics, rules, incidents, idempotency
-- notification-service owns: notification
CREATE SCHEMA IF NOT EXISTS metric;
CREATE SCHEMA IF NOT EXISTS notification;
