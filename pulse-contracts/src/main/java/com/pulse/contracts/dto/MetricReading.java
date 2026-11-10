package com.pulse.contracts.dto;

import java.time.Instant;

/**
 * Одна точка метрики в ответе {@code GET /users/{userId}/metrics} (FR-4, Этап 5).
 */
public record MetricReading(String name, Double value, Instant timestamp) {
}
