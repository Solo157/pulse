package com.pulse.metric.web;

import com.pulse.contracts.dto.IncidentResponse;
import com.pulse.contracts.dto.MetricReading;
import com.pulse.metric.exception.UserNotFoundException;
import com.pulse.metric.service.UserReadService;
import com.pulse.metric.service.UserService;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.UUID;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read API пользователя (FR-4, Этап 5): история метрик и инцидентов.
 * Доступ — только владельцем (Basic + совпадение {userId} в пути),
 * см. {@link OwnerReadAuthFilter}.
 */
@RestController
public class ReadController {

    private static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 1000;

    private final UserService userService;
    private final UserReadService userReadService;

    public ReadController(UserService userService, UserReadService userReadService) {
        this.userService = userService;
        this.userReadService = userReadService;
    }

    /**
     * История точек метрик из ClickHouse (по убыванию времени).
     *
     * @param metricName фильтр по имени метрики (опционально)
     * @param from       начало интервала включительно (ISO-8601), по умолчанию «с начала»
     * @param to         конец интервала включительно, по умолчанию «до сейчас»
     * @param limit      размер выборки, по умолчанию 100, максимум 1000
     *                   Выполняется асинхронно ({@code CompletableFuture} за TimeLimiter/
     *                   CircuitBreaker): Spring MVC удерживает запрос до завершения выборки.
     */
    @GetMapping("/users/{userId}/metrics")
    public CompletableFuture<List<MetricReading>> metrics(
            @PathVariable("userId") UUID userId,
            @RequestParam(name = "metric", required = false) String metricName,
            @RequestParam(name = "from", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(name = "to", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(name = "limit", required = false) Integer limit) {
        requireUser(userId);
        return userReadService.getHistory(userId, metricName,
                from == null ? Instant.EPOCH : from,
                to == null ? Instant.now().plus(1, ChronoUnit.DAYS) : to,
                clamp(limit));
    }

    /**
     * Инциденты пользователя из PostgreSQL (по убыванию времени открытия).
     *
     * @param from  начало интервала по времени открытия включительно, по умолчанию «с начала»
     * @param limit размер выборки, по умолчанию 100, максимум 1000
     */
    @GetMapping("/users/{userId}/incidents")
    public CompletableFuture<List<IncidentResponse>> incidents(
            @PathVariable("userId") UUID userId,
            @RequestParam(name = "from", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(name = "limit", required = false) Integer limit) {
        requireUser(userId);
        return userReadService.getIncidents(userId, from == null ? Instant.EPOCH : from, clamp(limit));
    }

    /** Лимит выборки: по умолчанию 100, жёсткая потолок 1000 (как в ingest). */
    private static int clamp(Integer limit) {
        if (limit == null || limit <= 0) {
            return DEFAULT_LIMIT;
        }
        return Math.min(limit, MAX_LIMIT);
    }

    private void requireUser(UUID userId) {
        if (!userService.userExists(userId)) {
            throw new UserNotFoundException("Пользователь " + userId + " не найден");
        }
    }
}
