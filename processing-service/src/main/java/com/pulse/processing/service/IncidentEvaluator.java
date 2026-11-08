package com.pulse.processing.service;

import com.pulse.contracts.common.MetricPoint;
import com.pulse.contracts.event.IncidentDetected;
import com.pulse.contracts.event.MetricEvent;
import com.pulse.contracts.event.SchemaVersions;
import com.pulse.processing.config.ProcessingProperties;
import com.pulse.processing.rule.MonitoringRule;
import com.pulse.processing.rule.RuleCache;
import com.pulse.processing.state.IncidentState;
import com.pulse.processing.state.IncidentStateRepository;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Оценка правил и state-машина инцидентов (раздел 8, согласование п. 10).
 *
 * <p>Правила берутся из кэша {@link RuleCache}, без обращения к БД на точку.
 * Переходы состояния выполняет {@link IncidentStateRepository} атомарными запросами.</p>
 *
 * <p>Обработка пакета — одна транзакция PostgreSQL: переход state-машины и запись
 * события в transactional outbox ({@link OutboxService}) фиксируются вместе,
 * поэтому сбой Kafka или падение сервиса между ними не теряет инцидент. При ошибке
 * транзакция откатывается целиком и Kafka доставит пакет заново (at-least-once);
 * повтор пакета не создаёт второго события — переход уже зафиксирован или откатился.</p>
 *
 * <p>Ограничения ДЕМО: закрытие возможно только по приходу «нормальной» точки
 * (по таймеру инцидент не закрывается); история периодов одного ключа не хранится.</p>
 */
@Service
public class IncidentEvaluator {

    private static final Logger log = LoggerFactory.getLogger(IncidentEvaluator.class);

    private final RuleCache ruleCache;
    private final IncidentStateRepository stateRepository;
    private final OutboxService outbox;
    private final ProcessingProperties properties;
    private final MeterRegistry meterRegistry;

    public IncidentEvaluator(RuleCache ruleCache, IncidentStateRepository stateRepository,
            OutboxService outbox, ProcessingProperties properties, MeterRegistry meterRegistry) {
        this.ruleCache = ruleCache;
        this.stateRepository = stateRepository;
        this.outbox = outbox;
        this.properties = properties;
        this.meterRegistry = meterRegistry;
    }

    /** Обрабатывает все точки пакета по всем подходящим правилам — одной транзакцией БД. */
    @Transactional
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
            } else if (stateRepository.openIncident(incidentKey, point.timestamp(), point.timestamp())) {
                outbox.enqueue(openEvent(userId, rule, incidentKey, point, requestId));
                count("opened");
                log.info("Открыт инцидент {}: value={} threshold={}",
                        incidentKey, point.value(), rule.threshold());
            }
            return;
        }

        if (state.open() && cooldownElapsed(state) && stateRepository.closeIncident(incidentKey)) {
            outbox.enqueue(closeEvent(userId, rule, incidentKey, point, requestId));
            count("closed");
            log.info("Закрыт инцидент {} (cooldown {} выдержан)", incidentKey, properties.closeCooldown());
        } else {
            count(violation ? "violation-new" : "normal");
        }
    }

    private IncidentDetected openEvent(UUID userId, MonitoringRule rule, String incidentKey,
            MetricPoint point, String requestId) {
        return new IncidentDetected(UUID.randomUUID().toString(), userId.toString(), rule.id(),
                incidentKey, false, rule.metricName(), rule.threshold(), point.value(),
                Instant.now(), point.timestamp(), SchemaVersions.CURRENT, requestId);
    }

    private IncidentDetected closeEvent(UUID userId, MonitoringRule rule, String incidentKey,
            MetricPoint point, String requestId) {
        return new IncidentDetected(UUID.randomUUID().toString(), userId.toString(), rule.id(),
                incidentKey, true, rule.metricName(), rule.threshold(), null,
                Instant.now(), point.timestamp(), SchemaVersions.CURRENT, requestId);
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
