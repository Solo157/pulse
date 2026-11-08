package com.pulse.processing.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.contracts.event.IncidentDetected;
import com.pulse.processing.service.IncidentPublisher.IncidentPublishException;
import com.pulse.processing.state.OutboxRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Transactional outbox событий {@code IncidentDetected} (раздел 8).
 *
 * <p>Переход state-машины и {@link #enqueue} выполняются одной транзакцией
 * с {@code metric.incident_state}: сбой Kafka или падение сервиса между шагами
 * больше не теряет событие. Оповещатель {@link #publishPending()} добирает
 * непроявленные записи после сбоя и при старте сервиса; повторная публикация
 * отдаёт байт-в-байт то же событие со стабильным {@code eventId} — потребители
 * идемпотентны.</p>
 */
@Service
public class OutboxService {

    private static final Logger log = LoggerFactory.getLogger(OutboxService.class);

    private final OutboxRepository outbox;
    private final IncidentPublisher publisher;
    private final ObjectMapper objectMapper;
    private final Counter publishedCounter;
    private final Counter failedCounter;

    public OutboxService(OutboxRepository outbox, IncidentPublisher publisher,
            ObjectMapper objectMapper, MeterRegistry meterRegistry) {
        this.outbox = outbox;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
        this.publishedCounter = meterRegistry.counter("pulse.processing.outbox.published");
        this.failedCounter = meterRegistry.counter("pulse.processing.outbox.failed");
    }

    /**
     * Ставит событие открытия/закрытия инцидента в очередь outbox.
     * Вызывается из той же транзакции, что и переход state-машины.
     *
     * @return id события — он же primary key записи outbox, стабилен при повторных публикациях
     */
    public UUID enqueue(IncidentDetected event) {
        String payload = serialize(event);
        UUID id = UUID.fromString(event.eventId());
        outbox.enqueue(id, event.userId(), payload);
        return id;
    }

    /** Сериализует событие в канонический payload outbox. */
    public String serialize(IncidentDetected event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Не удалось сериализовать IncidentDetected", failure);
        }
    }

    /**
     * Одна порция оповещения: забрать готовые записи, опубликовать с ожиданием
     * подтверждения брокера, отметить результат. Неудачник возвращается в очередь
     * с экспоненциальной паузой по числу попыток.
     *
     * @return число опубликованных событий
     */
    public int publishPending(int limit, Duration lock, Duration backoffBase, int maxBackoffShift) {
        List<OutboxRepository.Pending> claimed = outbox.claim(limit, lock);
        int published = 0;
        for (OutboxRepository.Pending pending : claimed) {
            try {
                publisher.publish(pending.messageKey(), pending.payload());
                outbox.markPublished(pending.id());
                publishedCounter.increment();
                published++;
            } catch (IncidentPublishException failure) {
                failedCounter.increment();
                outbox.markFailed(pending.id(),
                        backoff(backoffBase, Math.min(pending.attempts(), maxBackoffShift)),
                        failure.toString());
                log.warn("IncidentDetected {} не опубликован (попытка {}), повтор по паузе: {}",
                        pending.id(), pending.attempts(), failure.getMessage());
            }
        }
        return published;
    }

    /** Чистка reтеншена: опубликованные записи старше {@code retention} удаляются. */
    public int purgePublished(Duration retention) {
        int deleted = outbox.deletePublishedOlderThan(Instant.now().minus(retention));
        if (deleted > 0) {
            log.info("Outbox: удалено опубликованных записей старше {}: {}", retention, deleted);
        }
        return deleted;
    }

    /** Пауза повтора: backoffBase * 2^shift (shift растёт с числом попыток, ограниченно). */
    static Duration backoff(Duration backoffBase, int shift) {
        return backoffBase.multipliedBy(1L << shift);
    }
}
