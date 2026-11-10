package com.pulse.storage.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Настройки StorageService (префикс {@code pulse} в application.yml).
 *
 * @param metricsTopic топик Kafka для чтения (storage-group)
 */
@ConfigurationProperties(prefix = "pulse")
public record StorageProperties(String metricsTopic) {
}
