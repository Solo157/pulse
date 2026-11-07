package com.pulse.processing.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.contracts.event.IncidentDetected;
import com.pulse.contracts.event.SchemaVersions;
import com.pulse.processing.config.ProcessingProperties;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Публикация {@code IncidentDetected} в топик {@code incidents}
 * с key = {@code userId} (раздел 7: порядок и партиционирование по владельцу).
 *
 * <p>Публикация выполняется ДО фиксации перехода в {@code metric.incident_state}:
 * сбой между этими шагами даёт повтор события при повторной обработке,
 * но никогда — потерю инцидента (at-least-once; потребители идемпотентны).</p>
 */
@Component
public class IncidentPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final ProcessingProperties properties;

    public IncidentPublisher(KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper, ProcessingProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    /**
     * Публикует событие нарушения/закрытия и ждёт подтверждения брокера.
     *
     * @throws IncidentPublishException брокер не подтвердил запись за {@code pulse.publish-ack-timeout}
     */
    public void publish(String userId, String ruleId, boolean close, String metricName,
            double threshold, Double value, Instant sampleTime, String requestId) {
        IncidentDetected event = new IncidentDetected(
                UUID.randomUUID().toString(), userId, ruleId,
                userId + ":" + ruleId, close, metricName, threshold, value,
                Instant.now(), close ? null : sampleTime, SchemaVersions.CURRENT, requestId);
        String payload = serialize(event);
        try {
            kafkaTemplate.send(properties.incidentsTopic(), userId, payload)
                    .get(properties.publishAckTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IncidentPublishException("Прервано ожидание подтверждения Kafka", interrupted);
        } catch (ExecutionException | TimeoutException failure) {
            throw new IncidentPublishException("IncidentDetected не подтверждён брокером", failure);
        }
    }

    private String serialize(IncidentDetected event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Не удалось сериализовать IncidentDetected", failure);
        }
    }

    /** Сбой публикации события инцидента. */
    public static class IncidentPublishException extends RuntimeException {
        public IncidentPublishException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
