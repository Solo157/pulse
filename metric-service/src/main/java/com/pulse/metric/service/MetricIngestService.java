package com.pulse.metric.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.contracts.common.MetricPoint;
import com.pulse.metric.config.PulseProperties;
import com.pulse.contracts.dto.IngestResponse;
import com.pulse.contracts.dto.MetricBatchRequest;
import com.pulse.metric.exception.IdempotencyConflictException;
import com.pulse.metric.exception.PayloadTooLargeException;
import com.pulse.metric.exception.UserNotFoundException;
import com.pulse.metric.repository.IngestIdempotencyRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Приём пакета метрик (FR-2): проверка пользователя и лимитов, идемпотентность,
 * устойчивый приём в transactional outbox.
 *
 * <p>HTTP 202 означает, что пакет зафиксирован в PostgreSQL (запись журнала
 * идемпотентности + событие в очереди {@code ingest_outbox} — одна транзакция).
 * Доставка в Kafka выполняется из очереди at-least-once и не участвует в ответе:
 * недоступная Kafka задерживает доставку, но не превращает устойчивый приём в 503.</p>
 */
@Service
public class MetricIngestService {

    private static final Logger log = LoggerFactory.getLogger(MetricIngestService.class);

    private final UserService userService;
    private final IngestOutboxService outbox;
    private final IngestIdempotencyRepository idempotencyRepository;
    private final ObjectMapper objectMapper;
    private final int maxPointsPerRequest;
    private final Counter acceptedCounter;
    private final Counter rejectedCounter;
    private final Counter replayCounter;

    public MetricIngestService(UserService userService,
                               IngestOutboxService outbox,
                               IngestIdempotencyRepository idempotencyRepository,
                               ObjectMapper objectMapper,
                               PulseProperties properties,
                               MeterRegistry meterRegistry) {
        this.userService = userService;
        this.outbox = outbox;
        this.idempotencyRepository = idempotencyRepository;
        this.objectMapper = objectMapper;
        this.maxPointsPerRequest = properties.maxPointsPerRequest();
        this.acceptedCounter = meterRegistry.counter("pulse.ingest.accepted");
        this.rejectedCounter = meterRegistry.counter("pulse.ingest.rejected");
        this.replayCounter = meterRegistry.counter("pulse.ingest.idempotent_replay");
    }

    /**
     * Обрабатывает один пакет метрик. Весь шаг «приём» — одна транзакция PostgreSQL.
     *
     * @throws UserNotFoundException      пользователь не зарегистрирован (404)
     * @throws IllegalArgumentException   невалидный пакет (400)
     * @throws PayloadTooLargeException   слишком много точек (413)
     * @throws IdempotencyConflictException повтор с другим телом (409)
     */
    @Transactional
    public IngestResponse ingest(UUID userId, MetricBatchRequest request, String idempotencyKey,
                                 String requestId) {
        if (request == null || request.metrics() == null || request.metrics().isEmpty()) {
            rejectedCounter.increment();
            throw new IllegalArgumentException("Тело запроса не содержит metric-точек");
        }
        List<MetricPoint> points = request.metrics();
        if (points.size() > maxPointsPerRequest) {
            rejectedCounter.increment();
            throw new PayloadTooLargeException(
                    "Слишком большой пакет: " + points.size() + " точек, максимум " + maxPointsPerRequest);
        }
        validatePoints(points);
        if (!userService.userExists(userId)) {
            rejectedCounter.increment();
            throw new UserNotFoundException(userId.toString());
        }

        String requestHash = IngestRequestHasher.hash(objectMapper, points);
        String eventId = UUID.randomUUID().toString();
        if (!idempotencyRepository.tryCreate(idempotencyKey, userId,
                eventId, points.size(), requestHash)) {
            return handleExistingClaim(userId, idempotencyKey, requestHash);
        }
        outbox.accept(eventId, userId.toString(), points, requestId);
        acceptedCounter.increment(points.size());
        log.info("Пакет устойчиво принят: пользователь {}, событие {}, точек {}",
                userId, eventId, points.size());
        return new IngestResponse(eventId, userId.toString(), points.size());
    }

    /**
     * Ключ занят: тот же отпечаток тела — повтор, отвечаем прежним accepted-ответом
     * (на любом статусе: IN_PROGRESS тоже устойчиво принят, событие уже в очереди).
     * Другое тело под тем же ключом — 409.
     */
    private IngestResponse handleExistingClaim(UUID userId, String idempotencyKey, String requestHash) {
        List<IngestIdempotencyRepository.Row> rows = idempotencyRepository.find(idempotencyKey, userId);
        if (rows.isEmpty()) {
            // Гонка с политикой хранения: запись удалена между проверками. Повтор запроса.
            throw new IdempotencyConflictException();
        }
        IngestIdempotencyRepository.Row row = rows.get(0);
        if (!IngestRequestHasher.matches(row.requestHash(), requestHash)) {
            throw new IdempotencyConflictException(
                    "Ключ идемпотентности уже использован с другим телом запроса");
        }
        replayCounter.increment();
        log.debug("Повтор идемпотентного запроса {}, отдаём прежний ответ ({})",
                idempotencyKey, row.status());
        return new IngestResponse(row.eventId(), userId.toString(), row.acceptedCount());
    }

    private void validatePoints(List<MetricPoint> points) {
        for (MetricPoint point : points) {
            if (point == null || point.name() == null || point.name().isBlank()) {
                throw new IllegalArgumentException("У метрики отсутствует имя");
            }
            if (point.name().length() > 255) {
                throw new IllegalArgumentException("Слишком длинное имя метрики");
            }
            if (Double.isNaN(point.value()) || Double.isInfinite(point.value())) {
                throw new IllegalArgumentException("Некорректное значение метрики " + point.name());
            }
            if (point.timestamp() == null) {
                throw new IllegalArgumentException("У метрики " + point.name() + " отсутствует timestamp");
            }
        }
    }
}
