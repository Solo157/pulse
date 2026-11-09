package com.pulse.metric.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Настройки MetricService (префикс {@code pulse} в application.yml).
 *
 * @param topics           имена топиков Kafka
 * @param maxPointsPerRequest      максимум точек в одном запросе (сверх — 413)
 * @param maxRequestBodyBytes      максимум байт тела запроса (сверх — 413)
 * @param publishAckTimeout        сколько ждать подтверждения брокера перед 202
 */
@ConfigurationProperties(prefix = "pulse")
public record PulseProperties(Topics topics,
                              int maxPointsPerRequest,
                              long maxRequestBodyBytes,
                              Duration publishAckTimeout) {

    public record Topics(String metrics, String incidents) {
    }
}
