package com.pulse.metric.service;

import com.pulse.contracts.dto.IngestResponse;
import com.pulse.contracts.dto.MetricBatchRequest;
import com.pulse.contracts.common.MetricPoint;
import com.pulse.metric.config.PulseProperties;
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

/**
 * Приём пакета метрик (FR-2): проверка пользователя и лимитов, идемпотентность,
 * публикация с ожиданием подтверждения брокера.
 */
@Service
public class MetricIngestService {

    private static final Logger log = LoggerFactory.getLogger(MetricIngestService.class);

    private final UserService userService;
    private final MetricEventPublisher publisher;
    private final IngestIdempotencyRepository idempotencyRepository;
    private final int maxPointsPerRequest;
    private final Counter acceptedCounter;

    public MetricIngestService(UserService userService,
                               MetricEventPublisher publisher,
                               IngestIdempotencyRepository idempotencyRepository,
                               PulseProperties properties,
                               MeterRegistry meterRegistry) {
        this.userService = userService;
        this.publisher = publisher;
        this.idempotencyRepository = idempotencyRepository;
        this.maxPointsPerRequest = properties.maxPointsPerRequest();
        this.acceptedCounter = meterRegistry.counter("pulse.ingest.accepted");
    }

    /**
     * Обрабатывает один пакет метрик.
     *
     * @throws UserNotFoundException      пользователь не зарегистрирован (404)
     * @throws IllegalArgumentException    невалидный пакет (400)
     * @throws com.pulse.metric.exception.PayloadTooLargeException слишком много точек (413)
     * @throws IdempotencyConflictException повтор с незавершённым ключом (409)
     * @throws com.pulse.metric.exception.KafkaUnavailableException брокер не подтвердил запись (503)
     */
    public IngestResponse ingest(UUID userId, MetricBatchRequest request, String idempotencyKey,
                                 String requestId) {
        if (request == null || request.metrics() == null || request.metrics().isEmpty()) {
            throw new IllegalArgumentException("Тело запроса не содержит metric-точек");
        }
        List<MetricPoint> points = request.metrics();
        if (points.size() > maxPointsPerRequest) {
            throw new com.pulse.metric.exception.PayloadTooLargeException(
                    "Слишком большой пакет: " + points.size() + " точек, максимум " + maxPointsPerRequest);
        }
        validatePoints(points);
        if (!userService.userExists(userId)) {
            throw new UserNotFoundException(userId.toString());
        }

        if (!idempotencyRepository.tryCreate(idempotencyKey, userId)) {
            return handleExistingClaim(userId, idempotencyKey);
        }

        String eventId;
        try {
            eventId = publisher.publish(userId, points, requestId);
        } catch (RuntimeException exception) {
            // Публикация не подтверждена: убираем IN_PROGRESS, чтобы клиент мог повторить тот же ключ.
            idempotencyRepository.delete(idempotencyKey, userId);
            throw exception;
        }
        idempotencyRepository.markCompleted(idempotencyKey, userId, eventId, points.size());
        acceptedCounter.increment(points.size());
        log.info("Пакет принят: пользователь {}, событий {}, точек {}", userId, eventId, points.size());
        return new IngestResponse(eventId, userId.toString(), points.size());
    }

    private IngestResponse handleExistingClaim(UUID userId, String idempotencyKey) {
        List<IngestIdempotencyRepository.Row> rows = idempotencyRepository.find(idempotencyKey, userId);
        if (rows.isEmpty()) {
            // Гонка: запись появилась и пропала между проверками. Повтор с тем же ключом.
            throw new IdempotencyConflictException();
        }
        IngestIdempotencyRepository.Row row = rows.get(0);
        if (IngestIdempotencyRepository.STATUS_COMPLETED.equals(row.status())) {
            log.debug("Повтор идемпотентного запроса {}, отдаём прежний ответ", idempotencyKey);
            return new IngestResponse(row.eventId(), userId.toString(), row.acceptedCount());
        }
        throw new IdempotencyConflictException();
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
