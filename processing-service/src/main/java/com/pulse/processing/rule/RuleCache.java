package com.pulse.processing.rule;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Кэш правил мониторинга: список правил на имя метрики, обновляется по TTL
 * (согласование п. 10: никаких синхронных запросов к БД на каждую точку).
 * Пустой кэш до первой загрузки — это безопасно: точки без правил просто не проверяются.
 */
@Component
@EnableScheduling
public class RuleCache {

    private static final Logger log = LoggerFactory.getLogger(RuleCache.class);

    private final JdbcTemplate jdbcTemplate;

    private volatile Map<String, List<MonitoringRule>> rulesByMetricName = Map.of();
    private volatile boolean loaded;

    public RuleCache(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /** Обновление кэша с периодом {@code pulse.rule-cache-ttl}. */
    @Scheduled(fixedDelayString = "${pulse.rule-cache-ttl:30s}",
            initialDelayString = "${pulse.rule-cache-ttl:30s}")
    public void refresh() {
        try {
            List<MonitoringRule> rules = jdbcTemplate.query(
                    "SELECT id, metric_name, threshold FROM metric.monitoring_rules WHERE enabled",
                    (rs, rowNum) -> new MonitoringRule(rs.getString("id"),
                            rs.getString("metric_name"), rs.getDouble("threshold")));
            rulesByMetricName = Map.copyOf(rules.stream()
                    .collect(Collectors.groupingBy(MonitoringRule::metricName, Collectors.toList())));
            loaded = true;
        } catch (RuntimeException failure) {
            // Правило не загрузилось — работаем на предыдущем снимке; ошибка в логе заметна.
            log.error("Не удалось обновить кэш правил мониторинга", failure);
        }
    }

    /**
     * Активные правила для метрики; пустой список — метрика не мониторится.
     * Первая побеждённая точка могла прийти раньше плановой загрузки — тогда
     * грузим правила синхронно один раз.
     */
    public List<MonitoringRule> rulesFor(String metricName) {
        if (!loaded) {
            refresh();
        }
        return rulesByMetricName.getOrDefault(metricName, List.of());
    }
}
