package com.pulse.contracts.common;

import java.time.Instant;

/**
 * Одна точка измерения метрики в момент времени.
 * Используется и в HTTP API приёма метрик, и внутри Kafka-события. Неизменяемая.
 *
 * @param name      имя метрики, например "cpu_usage"
 * @param value     числовое значение измерения
 * @param timestamp время измерения в UTC; в JSON передаётся как строка ISO-8601
 */
public record MetricPoint(String name, double value, Instant timestamp) {
}
