-- Схема notification для Этапа 4. Идемпотентно, выполняется при старте сервиса.

CREATE SCHEMA IF NOT EXISTS metric;

-- Уведомления об инцидентах. Первичный ключ — идентификатор события IncidentDetected:
-- повторная доставка события Kafka не создаёт второе уведомление (idempotent, раздел 7).
-- Владелец claim()-строки выполняет доставку; остальные реплики её пропускают.
CREATE TABLE IF NOT EXISTS metric.notifications (
    source_event_id TEXT PRIMARY KEY,
    user_id         UUID NOT NULL,
    incident_key    TEXT NOT NULL,
    channel         TEXT NOT NULL,
    destination     TEXT NOT NULL,
    status          TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at         TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS notifications_user_time_idx
    ON metric.notifications (user_id, created_at);
