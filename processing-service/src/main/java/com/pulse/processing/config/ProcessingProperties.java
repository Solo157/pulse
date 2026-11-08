package com.pulse.processing.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Настройки обработки (префикс {@code pulse}). Topic'ы и тайминги согласованы в разделе 8/10:
 * одно нарушение — один открытый инцидент, закрытие не раньше cooldown после последней
 * нарушающей точки.
 *
 * @param metricsTopic        топик с пакетами метрик (источник)
 * @param incidentsTopic      топик событий {@code IncidentDetected} (сток)
 * @param closeCooldown       минимальный интервал «нарушение → нормализация» для закрытия
 * @param ruleCacheTtl        срок жизни кэша правил мониторинга (без запроса на каждую точку)
 * @param publishAckTimeout   ожидание подтверждения брокера при публикации события
 * @param outboxPollInterval  период оповещателя outbox; он же база паузы повтора (2^n)
 * @param outboxBatchSize     максимум записей outbox в одной порции оповещения
 * @param outboxRetention     срок хранения уже опубликованных записей outbox
 */
@ConfigurationProperties(prefix = "pulse")
public record ProcessingProperties(
        String metricsTopic,
        String incidentsTopic,
        Duration closeCooldown,
        Duration ruleCacheTtl,
        Duration publishAckTimeout,
        Duration outboxPollInterval,
        int outboxBatchSize,
        Duration outboxRetention) {
}
