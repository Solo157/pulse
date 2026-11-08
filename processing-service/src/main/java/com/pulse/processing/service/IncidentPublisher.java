package com.pulse.processing.service;

import com.pulse.processing.config.ProcessingProperties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Отправка готового payload события {@code IncidentDetected} в топик {@code incidents}
 * с key = {@code userId} (раздел 7: порядок и партиционирование по владельцу).
 *
 * <p>Вызывается только оповещателем {@link OutboxService}: событие уже записано
 * в transactional outbox одной транзакцией с переходом state-машины, поэтому
 * недоступность Kafka здесь задерживает публикацию, но не теряет событие.</p>
 */
@Component
public class IncidentPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ProcessingProperties properties;

    public IncidentPublisher(KafkaTemplate<String, String> kafkaTemplate,
            ProcessingProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    /**
     * Публикует payload и ждёт подтверждения брокера.
     *
     * @throws IncidentPublishException брокер не подтвердил запись за {@code pulse.publish-ack-timeout}
     */
    public void publish(String messageKey, String payload) {
        try {
            kafkaTemplate.send(properties.incidentsTopic(), messageKey, payload)
                    .get(properties.publishAckTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IncidentPublishException("Прервано ожидание подтверждения Kafka", interrupted);
        } catch (ExecutionException | TimeoutException failure) {
            throw new IncidentPublishException("IncidentDetected не подтверждён брокером", failure);
        } catch (org.springframework.kafka.KafkaException failure) {
            // Синхронный отказ KafkaTemplate (например, метаданные топика не получены
            // за max.block.ms при недоступном кластере).
            throw new IncidentPublishException("IncidentDetected не отправлен в Kafka", failure);
        }
    }

    /** Сбой публикации события инцидента. */
    public static class IncidentPublishException extends RuntimeException {
        public IncidentPublishException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
