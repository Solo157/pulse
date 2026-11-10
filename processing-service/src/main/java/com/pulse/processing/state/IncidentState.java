package com.pulse.processing.state;

import java.time.Instant;
import java.util.UUID;

/**
 * Строка состояния инцидента по бизнес-ключу {@code userId + ":" + ruleId}
 * (таблица {@code metric.incident_state}).
 *
 * @param userId         владелец метрик
 * @param ruleId         идентификатор правила
 * @param metricName     имя метрики правила
 * @param open           открыт ли инцидент сейчас
 * @param lastViolationAt время последней нарушающей точки (для cooldown закрытия)
 * @param lastSampleAt   время последней обработанной точки (фильтр out-of-order)
 */
public record IncidentState(
        UUID userId,
        String ruleId,
        String metricName,
        boolean open,
        Instant lastViolationAt,
        Instant lastSampleAt) {
}
