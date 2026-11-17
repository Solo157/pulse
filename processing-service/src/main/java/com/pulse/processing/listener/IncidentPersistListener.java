package com.pulse.processing.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.contracts.event.IncidentDetected;
import com.pulse.processing.state.IncidentRowRepository;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Группа {@code metric-group}: читает топик {@code incidents} и сохраняет
 * инциденты в {@code metric.incidents} (раздел 6.4/FR-4).
 * Операционно изолирована от processing-group: даже если обработка правил отстала,
 * уже опубликованные инциденты не теряются.
 */
@Component
public class IncidentPersistListener {

    private static final Logger log = LoggerFactory.getLogger(IncidentPersistListener.class);

    private final IncidentRowRepository rowRepository;
    private final ObjectMapper objectMapper;

    public IncidentPersistListener(IncidentRowRepository rowRepository, ObjectMapper objectMapper) {
        this.rowRepository = rowRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(id = "metric-group-incidents", topics = "${pulse.incidents-topic}",
            groupId = "metric-group")
    public void onIncidentDetected(String payload) {
        IncidentDetected event = parse(payload);
        UUID userId = UUID.fromString(event.userId());
        if (event.close()) {
            rowRepository.close(event.incidentKey(), event.createdAt());
            log.debug("Инцидент {} закрыт", event.incidentKey());
        } else {
            rowRepository.open(UUID.fromString(event.eventId()), event.incidentKey(), userId,
                    event.ruleId(), event.metricName(), event.value(), event.threshold(),
                    event.createdAt());
            log.info("Инцидент {} записан (value={}, threshold={})",
                    event.incidentKey(), event.value(), event.threshold());
        }
    }

    private IncidentDetected parse(String payload) {
        try {
            return objectMapper.readValue(payload, IncidentDetected.class);
        } catch (Exception failure) {
            throw new IllegalArgumentException("Не разобрано событие IncidentDetected: " + payload, failure);
        }
    }
}
