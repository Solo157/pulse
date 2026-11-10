-- Схема processing для Этапа 3. Идемпотентно (IF NOT EXISTS), выполняется при старте.
-- Чужие таблицы этапов (users, ingest_idempotency) создаёт schema.sql MetricService.

CREATE SCHEMA IF NOT EXISTS metric;

-- Состояние инцидентов по бизнес-ключу (userId + ":" + ruleId). Одна строка на
-- непрерывный период нарушения (согласование п. 10): open=false — инцидент закрыт,
-- строка остаётся как память о period, повторное нарушение обновляет её же.
-- last_sample_at — фильтр out-of-order: более старые точки игнорируются (п. 10).
-- last_violation_at — момент последней нарушающей точки; закрытие возможно только
-- когда следующая "нормальная" точка пришла не раньше, чем через pulse.close-cooldown
-- после неё. Если поток метрик остановился — инцидент остаётся открытым
-- (ограничение ДЕМО, закрытие по таймеру не реализуем).
CREATE TABLE IF NOT EXISTS metric.incident_state (
    incident_key      TEXT PRIMARY KEY,
    user_id           UUID NOT NULL,
    rule_id           TEXT NOT NULL,
    metric_name       TEXT NOT NULL,
    open              BOOLEAN NOT NULL DEFAULT FALSE,
    opened_at         TIMESTAMPTZ,
    last_violation_at TIMESTAMPTZ,
    last_sample_at    TIMESTAMPTZ
);

-- Хранилище инцидентов: пишет группа metric-group (Этап 3), читает API истории (Этап 5).
-- Уникальность по incident_key: строка живёт всё время жизни объекта "инцидент",
-- открытие/закрытие меняют status/opened_at/closed_at; история периодов в ДЕМО не хранится.
-- ALTER + индекс — миграция для БД, созданных на прежних этапах без incident_key.
CREATE TABLE IF NOT EXISTS metric.incidents (
    id           UUID PRIMARY KEY,
    user_id      UUID NOT NULL,
    rule_id      TEXT NOT NULL,
    metric_name  TEXT NOT NULL,
    incident_key TEXT,
    status       TEXT NOT NULL,
    value        DOUBLE PRECISION,
    threshold    DOUBLE PRECISION,
    opened_at    TIMESTAMPTZ NOT NULL,
    closed_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS incidents_user_time_idx
    ON metric.incidents (user_id, opened_at);
ALTER TABLE metric.incidents ADD COLUMN IF NOT EXISTS incident_key TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS incidents_incident_key_uidx
    ON metric.incidents (incident_key);

-- Правила мониторинга: владелец — ProcessingService (раздел 10). В ДЕМО одно правило,
-- оператор один: value > threshold.
CREATE TABLE IF NOT EXISTS metric.monitoring_rules (
    id          TEXT PRIMARY KEY,
    metric_name TEXT NOT NULL,
    threshold   DOUBLE PRECISION NOT NULL,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE
);
INSERT INTO metric.monitoring_rules (id, metric_name, threshold)
VALUES ('temperature-high', 'temperature', 80.0)
ON CONFLICT (id) DO NOTHING;
