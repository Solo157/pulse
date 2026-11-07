package com.pulse.contracts.event;

import java.time.Instant;

/**
 * Событие нарушения правила мониторинга: публикуется ProcessingService
 * в топик {@code incidents} (FR-5, раздел 6.4).
 *
 * <p>Читают его группы {@code metric-group} (сохраняет инцидент в PostgreSQL)
 * и {@code notification-group} (создаёт уведомление).</p>
 *
 * <p>Семантика открытия/закрытия (раздел 8): на непрерывный период нарушения
 * открывается один инцидент; {@code incidentKey = userId + ":" + ruleId} —
 * бизнес-идентификатор открытого инцидента; {@code close = false} открывает,
 * {@code close = true} закрывает после cooldown. Повторные доставки отсекаются
 * потребителями по {@link #eventId}.</p>
 *
 * @param eventId       идентификатор события для дедупликации потребителей
 * @param userId        владелец нарушенной метрики
 * @param ruleId        идентификатор нарушенного правила
 * @param incidentKey   бизнес-ключ инцидента: userId + ":" + ruleId
 * @param close         false — инцидент открыт, true — закрыт (значение вернулось в норму)
 * @param metricName    метрика, на которой сработало правило
 * @param threshold     настроенный порог правила
 * @param value         значение метрики, нарушившее порог (при закрытии — нет)
 * @param createdAt     момент публикации события (UTC)
 * @param sampleTime    время точки метрики, вызвавшей срабатывание (при закрытии — нет)
 * @param schemaVersion версия схемы полезной нагрузки (см. {@link SchemaVersions})
 * @param requestId     correlation id для сквозной трассировки
 */
public record IncidentDetected(
        String eventId,
        String userId,
        String ruleId,
        String incidentKey,
        boolean close,
        String metricName,
        double threshold,
        Double value,
        Instant createdAt,
        Instant sampleTime,
        String schemaVersion,
        String requestId) {
}
