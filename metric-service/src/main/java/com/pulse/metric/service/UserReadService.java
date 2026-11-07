package com.pulse.metric.service;

import com.pulse.contracts.dto.IncidentResponse;
import com.pulse.contracts.dto.MetricReading;
import com.pulse.metric.repository.IncidentReadRepository;
import com.pulse.metric.repository.MetricHistoryRepository;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;

/**
 * Пользовательское чтение (FR-4, Этап 5): история метрик и инцидентов владельца.
 * Отдельный сервис: в {@code UserService} остаётся только регистрация и проверка учётных
 * данных (CRUD/аутентификация), здесь — выборки для read API.
 */
@Service
public class UserReadService {

    private final MetricHistoryRepository metricHistoryRepository;
    private final IncidentReadRepository incidentReadRepository;

    public UserReadService(MetricHistoryRepository metricHistoryRepository,
                           IncidentReadRepository incidentReadRepository) {
        this.metricHistoryRepository = metricHistoryRepository;
        this.incidentReadRepository = incidentReadRepository;
    }

    public List<MetricReading> getHistory(UUID userId, String metricName, Instant from,
                                          Instant to, int limit) {
        return metricHistoryRepository.find(userId, metricName, from, to, limit);
    }

    public List<IncidentResponse> getIncidents(UUID userId, Instant from, int limit) {
        return incidentReadRepository.find(userId, from, limit);
    }
}
