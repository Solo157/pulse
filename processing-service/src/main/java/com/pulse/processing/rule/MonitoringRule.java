package com.pulse.processing.rule;

/**
 * Правило мониторинга из {@code metric.monitoring_rules}.
 * В ДЕМО единственный оператор — «значение больше порога» (раздел 10).
 *
 * @param id          стабильный идентификатор правила (например, {@code temperature-high})
 * @param metricName  имя метрики, к которой применяется правило
 * @param threshold   порог; инцидент открывается при {@code value > threshold}
 */
public record MonitoringRule(String id, String metricName, double threshold) {
}
