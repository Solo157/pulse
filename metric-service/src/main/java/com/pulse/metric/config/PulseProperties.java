package com.pulse.metric.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Настройки MetricService (префикс {@code pulse} в application.yml).
 *
 * @param topics           имена топиков Kafka
 * @param maxPointsPerRequest      максимум точек в одном запросе (сверх — 413)
 * @param maxRequestBodyBytes      максимум байт тела запроса (сверх — 413)
 * @param publishAckTimeout        сколько ждать подтверждения брокера при отправке из очереди
 * @param outboxPollInterval       период отправителя outbox; он же база паузы повтора (2^n)
 * @param outboxBatchSize          максимум событий outbox в одной порции отправки
 * @param outboxRetention          срок хранения опубликованных записей outbox
 * @param idempotencyRetention     срок хранения завершённых записей журнала идемпотентности
 */
@ConfigurationProperties(prefix = "pulse")
public record PulseProperties(Topics topics,
                              int maxPointsPerRequest,
                              long maxRequestBodyBytes,
                              Duration publishAckTimeout,
                              Duration outboxPollInterval,
                              int outboxBatchSize,
                              Duration outboxRetention,
                              Duration idempotencyRetention) {

    public record Topics(String metrics, String incidents) {
    }
}
