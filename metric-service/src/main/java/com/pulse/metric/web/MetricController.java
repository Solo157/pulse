package com.pulse.metric.web;

import com.pulse.contracts.dto.IngestResponse;
import com.pulse.contracts.dto.MetricBatchRequest;
import com.pulse.contracts.dto.RegisterRequest;
import com.pulse.contracts.dto.RegisterResponse;
import com.pulse.metric.config.RequestIdFilter;
import com.pulse.metric.service.MetricIngestService;
import com.pulse.metric.service.UserService;
import java.util.UUID;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * HTTP API MetricService: регистрация и приём пакетов метрик (раздел 9).
 */
@RestController
public class MetricController {

    private final UserService userService;
    private final MetricIngestService ingestService;

    public MetricController(UserService userService, MetricIngestService ingestService) {
        this.userService = userService;
        this.ingestService = ingestService;
    }

    /** Регистрация пользователя: 201 + userId (FR-1). */
    @PostMapping("/auth/register")
    @ResponseStatus(HttpStatus.CREATED)
    public RegisterResponse register(@RequestBody RegisterRequest request) {
        return userService.register(request.email(), request.password());
    }

    /**
     * Приём пакета метрик: 202 только после подтверждения Kafka (FR-2).
     * Заголовок Idempotency-Key обязателен (без него Spring вернёт 400).
     */
    @PostMapping("/users/{userId}/metrics")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public IngestResponse ingest(@PathVariable("userId") UUID userId,
                                 @RequestBody MetricBatchRequest request,
                                 @RequestHeader("Idempotency-Key") String idempotencyKey) {
        return ingestService.ingest(userId, request, idempotencyKey,
                MDC.get(RequestIdFilter.REQUEST_ID_MDC_KEY));
    }
}
