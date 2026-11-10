package com.pulse.metric.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.contracts.common.MetricPoint;
import com.pulse.contracts.event.MetricEvent;
import com.pulse.contracts.event.SchemaVersions;
import com.pulse.metric.config.PulseProperties;
import com.pulse.metric.exception.KafkaUnavailableException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Публикует MetricEvent в топик metrics и ждёт подтверждения брокера:
 * метод завершается успешно только после ack (условие 202 по FR-2).
 */
@Service
public class MetricEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(MetricEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String metricsTopic;
    private final Duration publishAckTimeout;

    public MetricEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                                ObjectMapper objectMapper,
                                PulseProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.metricsTopic = properties.topics().metrics();
        this.publishAckTimeout = properties.publishAckTimeout();
    }

    /**
     * Собирает событие из точек и публикует его с key = userId.
     *
     * @return eventId созданного события
     * @throws KafkaUnavailableException если брокер не подтвердил запись за publishAckTimeout
     */
    public String publish(UUID userId, List<MetricPoint> points, String requestId) {
        String eventId = UUID.randomUUID().toString();
        MetricEvent event = new MetricEvent(eventId, userId.toString(), SchemaVersions.CURRENT,
                Instant.now(), requestId, points);
        String payload = toJson(event);
        try {
            kafkaTemplate.send(metricsTopic, userId.toString(), payload)
                    .get(publishAckTimeout.toMillis(), TimeUnit.MILLISECONDS);
            log.debug("Событие {} пользователя {} записано в Kafka", eventId, userId);
            return eventId;
        } catch (Exception exception) {
            throw new KafkaUnavailableException(exception);
        }
    }

    private String toJson(MetricEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Не удалось сериализовать MetricEvent", exception);
        }
    }
}
