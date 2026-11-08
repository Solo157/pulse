package com.pulse.metric.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.contracts.common.MetricPoint;
import com.pulse.contracts.event.MetricEvent;
import com.pulse.contracts.event.SchemaVersions;
import com.pulse.metric.repository.IngestIdempotencyRepository;
import com.pulse.metric.repository.IngestOutboxRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.KafkaException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Transactional outbox входящих пакетов метрик.
 *
 * * <p>Постановка события в очередь выполняется одной транзакцией с записью
 * журнала идемпотентности (см. {@link MetricIngestService}): созданная пара
 * (приём, событие) и есть основание для HTTP 202 — «устойчиво принято».
 * Отправитель {@link #publishPending()} добирает события в Kafka потом: после сбоя
 * брокера или падения процесса между приёмом и публикацией; повторная отправка
 * байт-в-байт идентична (eventId — primary key очереди), storage идемпотентен
 * по eventId.</p>
 */
@Service
public class IngestOutboxService {

    private static final Logger log = LoggerFactory.getLogger(IngestOutboxService.class);

    private final IngestOutboxRepository outbox;
    private final IngestIdempotencyRepository idempotency;
    private final Publisher publisher;
    private final ObjectMapper objectMapper;
    private final Counter publishedCounter;
    private final Counter failedCounter;

    public IngestOutboxService(IngestOutboxRepository outbox, IngestIdempotencyRepository idempotency,
            Publisher publisher, ObjectMapper objectMapper, MeterRegistry meterRegistry) {
        this.outbox = outbox;
        this.idempotency = idempotency;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
        this.publishedCounter = meterRegistry.counter("pulse.kafka.publish.success", "topic",
                "metrics");
        this.failedCounter = meterRegistry.counter("pulse.kafka.publish.failure", "topic", "metrics");
    }

    /**
     * Устойчиво принимает пакет: сериализует MetricEvent с заранее выданным eventId
     * и кладёт в outbox. Вызывается из той же транзакции, что и создание записи
     * идемпотентности.
     */
    public void accept(String eventId, String userId, List<MetricPoint> points, String requestId) {
        MetricEvent event = new MetricEvent(eventId, userId, SchemaVersions.CURRENT,
                Instant.now(), requestId, points);
        outbox.enqueue(eventId, userId, serialize(event));
    }

    public String serialize(MetricEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Не удалось сериализовать MetricEvent", failure);
        }
    }

    /**
     * Одна порция отправки. Перед порцией — сверка статусов (переживаем падение
     * ровно между подтверждением брокера и закрытием журнала). Подтверждение
     * брокера закрывает outbox и приём одной транзакцией; неудача — возврат
     * записи в очередь с удвоенной паузой.
     *
     * @return число опубликованных событий
     */
    public int publishPending(int limit, Duration lock, Duration backoffBase, int maxBackoffShift) {
        int repaired = reconcile();
        List<IngestOutboxRepository.Pending> claimed = outbox.claim(limit, lock);
        int published = 0;
        for (IngestOutboxRepository.Pending pending : claimed) {
            try {
                publisher.publish(pending.messageKey(), pending.payload());
                publisher.closePublished(pending.eventId());
                publishedCounter.increment();
                published++;
            } catch (KafkaException failure) {
                failedCounter.increment();
                outbox.markFailed(pending.eventId(),
                        backoff(backoffBase, Math.min(pending.attempts(), maxBackoffShift)),
                        failure.toString());
                log.warn("MetricEvent {} не опубликован (попытка {}), повтор по паузе: {}",
                        pending.eventId(), pending.attempts(), failure.getMessage());
            }
        }
        if (repaired > 0) {
            log.debug("Outbox ingest: опубликовано {}, восстановлено статусов {}", published, repaired);
        }
        return published;
    }

    /** Догоняет статус приёма, застрявший IN_PROGRESS из-за падения сразу после ack. */
    public int reconcile() {
        return publisher.reconcileStatuses();
    }

    /** Политика хранения: опубликованный outbox и завершённые строки журнала. */
    public void purge(Duration outboxRetention, Duration journalRetention) {
        int out = outbox.deletePublishedOlderThan(outboxRetention);
        int journal = idempotency.deleteCompletedOlderThan(journalRetention);
        if (out + journal > 0) {
            log.info("Outbox ingest: удалено записей outbox {}, журнала {}", out, journal);
        }
    }

    static Duration backoff(Duration backoffBase, int shift) {
        return backoffBase.multipliedBy(1L << shift);
    }

    /**
     * Отправка и атомарное закрытие публикации — отдельный бин, чтобы
     * {@code @Transactional} применялся (прокси не перехватывает self-invocation).
     */
    @Service
    public static class Publisher {

        private final MetricEventPublisher kafkaPublisher;
        private final IngestOutboxRepository outbox;
        private final IngestIdempotencyRepository idempotency;

        Publisher(MetricEventPublisher kafkaPublisher, IngestOutboxRepository outbox,
                IngestIdempotencyRepository idempotency) {
            this.kafkaPublisher = kafkaPublisher;
            this.outbox = outbox;
            this.idempotency = idempotency;
        }

        public void publish(String messageKey, String payload) {
            kafkaPublisher.publish(messageKey, payload);
        }

        /** Брокер подтвердил: outbox и журнал идемпотентности закрываются вместе. */
        @Transactional
        public void closePublished(String eventId) {
            outbox.markPublished(eventId);
            idempotency.markCompleted(eventId);
        }

        @Transactional
        public int reconcileStatuses() {
            return idempotency.reconcileCompleted();
        }
    }
}
