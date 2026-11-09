package com.pulse.processing.service;

import com.pulse.contracts.common.MetricPoint;
import com.pulse.contracts.event.MetricEvent;
import com.pulse.processing.config.ProcessingProperties;
import com.pulse.processing.rule.MonitoringRule;
import com.pulse.processing.rule.RuleCache;
import com.pulse.processing.state.IncidentState;
import com.pulse.processing.state.IncidentStateRepository;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Оценка правил и state-машина инцидентов (раздел 8, согласование п. 10).
 *
 * <p>Правила берутся из кэша {@link RuleCache}, без обращения к БД на точку.
 * Переходы состояния выполняет {@link IncidentStateRepository} атомарными запросами.</p>
 *
 * <p>Ограничения ДЕМО: закрытие возможно только по приходу «нормальной» точки
 * (по таймеру инцидент не закрывается); история периодов одного ключа не хранится.</p>
 */
@Service
public class IncidentEvaluator {

    private static final Logger log = LoggerFactory.getLogger(IncidentEvaluator.class);

    private final RuleCache ruleCache;
    private final IncidentStateRepository stateRepository;
    private final IncidentPublisher publisher;
    private final ProcessingProperties properties;
    private final MeterRegistry meterRegistry;

    public IncidentEvaluator(RuleCache ruleCache, IncidentStateRepository stateRepository,
            IncidentPublisher publisher, ProcessingProperties properties, MeterRegistry meterRegistry) {
        this.ruleCache = ruleCache;
        this.stateRepository = stateRepository;
        this.publisher = publisher;
        this.properties = properties;
        this.meterRegistry = meterRegistry;
    }

    /** Обрабатывает все точки пакета метрик по всем подходящим правилам. */
    public void process(MetricEvent event) {
        UUID userId = UUID.fromString(event.userId());
        for (MetricPoint point : event.points()) {
            for (MonitoringRule rule : ruleCache.rulesFor(point.name())) {
                processPoint(userId, rule, point, event.requestId());
            }
        }
    }

    private void processPoint(UUID userId, MonitoringRule rule, MetricPoint point, String requestId) {
        String incidentKey = userId + ":" + rule.id();
        Optional<IncidentState> fresh = stateRepository.registerSample(
                incidentKey, userId, rule.id(), rule.metricName(), point.timestamp());
        if (fresh.isEmpty()) {
            count("stale");
            return;
        }
        IncidentState state = fresh.get();
        boolean violation = point.value() > rule.threshold();

        if (violation) {
            if (state.open()) {
                stateRepository.registerViolation(incidentKey, point.timestamp());
                count("violation-continued");
            } else {
                stateRepository.openIncident(incidentKey, point.timestamp(), point.timestamp());
                publisher.publish(userId.toString(), rule.id(), false, rule.metricName(),
                        rule.threshold(), point.value(), point.timestamp(), requestId);
                count("opened");
                log.info("Открыт инцидент {}: value={} threshold={}",
                        incidentKey, point.value(), rule.threshold());
            }
            return;
        }

        if (state.open() && cooldownElapsed(state)) {
            stateRepository.closeIncident(incidentKey);
            publisher.publish(userId.toString(), rule.id(), true, rule.metricName(),
                    rule.threshold(), null, point.timestamp(), requestId);
            count("closed");
            log.info("Закрыт инцидент {} (cooldown {} выдержан)", incidentKey, properties.closeCooldown());
        } else {
            count(violation ? "violation-new" : "normal");
        }
    }

    /** Нормализация тем дольше cooldown от последней нарушающей точки — инцидент закрываем. */
    private boolean cooldownElapsed(IncidentState state) {
        if (state.lastViolationAt() == null) {
            // Последовательность open->registerViolation атомарна; null означает только
            // повреждение данных — закрываем, иначе инцидент не закроется никогда.
            return true;
        }
        return Duration.between(state.lastViolationAt(), state.lastSampleAt())
                .compareTo(properties.closeCooldown()) >= 0;
    }

    private void count(String outcome) {
        meterRegistry.counter("pulse.processing.points", "outcome", outcome).increment();
    }
}
