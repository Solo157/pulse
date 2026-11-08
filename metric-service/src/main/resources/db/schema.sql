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
-- (user, заголовок Idempotency-Key). Статусы: IN_PROGRESS — событие устойчиво
-- принято (есть в outbox) и ждёт подтверждения брокером, COMPLETED — брокер
-- подтвердил. eventId и accepted_count фиксируются при создании, поэтому повтор
-- с тем же ключом (и тем же телом) отвечает тем же eventId на любом статусе.
-- request_hash — отпечаток тела запроса: повтор с тем же ключом, но другим
-- телом даёт 409. Записи COMPLETED старше pulse.idempotency-retention удаляются.
CREATE TABLE IF NOT EXISTS metric.ingest_idempotency (
    idempotency_key TEXT        NOT NULL,
    user_id         UUID        NOT NULL,
    status          TEXT        NOT NULL,
    event_id        TEXT,
    accepted_count  INTEGER,
    request_hash    TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (idempotency_key, user_id)
);

-- Et5: колонка password_hash добавляется идемпотентно (миграция существующих БД).
ALTER TABLE metric.users ADD COLUMN IF NOT EXISTS password_hash TEXT;
-- MIGRATION: idempotency + transactional outbox (P0-2) для БД, созданных на прежних этапах.
ALTER TABLE metric.ingest_idempotency ADD COLUMN IF NOT EXISTS request_hash TEXT;

-- Transactional outbox входящих MetricEvent (P0-2): строка outbox и запись
-- идемпотентности создаются одной транзакцией. HTTP 202 означает устойчивый
-- приём сюда; отдельный отправитель добирает события в Kafka at-least-once
-- (paуза повтора удваивается, держание lock_until переживает падение отправителя).
--eventId = primary key: повторная доставка байт-в-байт идентична, storage
-- идемпотентен по eventId (движок таблицы убирает дубли).
CREATE TABLE IF NOT EXISTS metric.ingest_outbox (
    event_id        TEXT PRIMARY KEY,
    message_key     TEXT NOT NULL,
    payload         TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,
    attempts        INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error      TEXT,
    lock_until      TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ingest_outbox_pending_idx
    ON metric.ingest_outbox (next_attempt_at) WHERE published_at IS NULL;
