package com.pulse.contracts.dto;

import com.pulse.contracts.common.MetricPoint;
import java.util.List;

/**
 * Тело запроса {@code POST /users/{userId}/metrics} (FR-2).
 * Один запрос содержит пакет метрик (до 1000 точек и до 1 МБ).
 */
public record MetricBatchRequest(List<MetricPoint> metrics) {
}
