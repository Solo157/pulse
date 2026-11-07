-- Схема metric. Принадлежит MetricService (разделы 6.2, 6.7).
-- Выполняется при старте каждого экземпляра (spring.sql.init, mode = always),
-- все операторы идемпотентны (IF NOT EXISTS).

CREATE SCHEMA IF NOT EXISTS metric;

-- Пользователи. Пароль хранится BCrypt-хешем (FR/Et5: чтение только владельцем);
-- API-ключ в ДЕМО не используется.
CREATE TABLE IF NOT EXISTS metric.users (
    id         UUID PRIMARY KEY,
    email      TEXT NOT NULL UNIQUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    password_hash TEXT
);

-- Идемпотентность ingest (раздел 7, пункт 4 согласования): один шаг "приём" на
-- (user, заголовок Idempotency-Key). Статусы: IN_PROGRESS — publish не завершён,
-- COMPLETED — событие подтверждено брокером, eventId публикуется повторно из строки.
CREATE TABLE IF NOT EXISTS metric.ingest_idempotency (
    idempotency_key TEXT        NOT NULL,
    user_id         UUID        NOT NULL,
    status          TEXT        NOT NULL,
    event_id        TEXT,
    accepted_count  INTEGER,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (idempotency_key, user_id)
);

-- Et5: колонка password_hash добавляется идемпотентно (миграция существующих БД).
ALTER TABLE metric.users ADD COLUMN IF NOT EXISTS password_hash TEXT;
