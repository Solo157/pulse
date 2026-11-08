package com.pulse.metric.service;

import com.pulse.metric.config.PulseProperties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Отправка готового payload {@code MetricEvent} в топик metrics с key = userId.
 *
 * <p>Вызывается только отправителем outbox ({@link IngestOutboxService}): событие
 * уже устойчиво принято в PostgreSQL (HTTP 202 выдан по факту записи в
 * transactional outbox), поэтому недоступная Kafka задерживает доставку,
 * но не отменяет приём и не приводит к дублям — eventId первичный ключ очереди.</p>
 */
@Service
public class MetricEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String metricsTopic;
    private final java.time.Duration publishAckTimeout;

    public MetricEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                                PulseProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.metricsTopic = properties.topics().metrics();
        this.publishAckTimeout = properties.publishAckTimeout();
    }

    /**
     * Публикует payload и ждёт подтверждения брокера.
     *
     * @throws org.springframework.kafka.KafkaException брокер не подтвердил запись
     *         (таймаут ожидания либо синхронный отказ отправки)
     */
    public void publish(String messageKey, String payload) {
        try {
            kafkaTemplate.send(metricsTopic, messageKey, payload)
                    .get(publishAckTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new org.springframework.kafka.KafkaException("Прервано ожидание подтверждения Kafka");
        } catch (ExecutionException | TimeoutException failure) {
            throw new org.springframework.kafka.KafkaException(
                    "MetricEvent не подтверждён брокером", failure);
        }
    }
}
