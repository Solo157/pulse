-- Минимум таблицы пользователей MetricService для интеграционных тестов уведомлений.
CREATE SCHEMA IF NOT EXISTS metric;
CREATE TABLE IF NOT EXISTS metric.users (
    id         UUID PRIMARY KEY,
    email      TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
