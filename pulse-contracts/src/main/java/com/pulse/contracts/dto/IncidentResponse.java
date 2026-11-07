package com.pulse.contracts.dto;

import java.time.Instant;

/**
 * Инцидент в том виде, в котором он отдаётся клиенту из
 * {@code GET /users/{userId}/incidents} (FR-4, раздел 9).
 *
 * @param status    {@code OPEN} или {@code CLOSED}
 * @param value     значение метрики, вызвавшее срабатывание (при закрытии может отсутствовать)
 * @param threshold порог правила, при котором открыт инцидент
 */
public record IncidentResponse(
        String userId,
        String ruleId,
        String metricName,
        String status,
        Double value,
        Double threshold,
        Instant openedAt,
        Instant closedAt) {
}
