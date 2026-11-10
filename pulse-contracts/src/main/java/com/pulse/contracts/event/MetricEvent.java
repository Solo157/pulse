package com.pulse.contracts.event;

import com.pulse.contracts.common.MetricPoint;
import java.time.Instant;
import java.util.List;

/**
 * Событие пакета метрик: публикуется MetricService в топик {@code metrics}
 * после подтверждения записи брокером (FR-2, 6.6).
 *
 * <p>Key сообщения в Kafka — {@code userId}, чтобы точки одного пользователя
 * попадали в одну партицию и читались потребителями в исходном порядке.</p>
 *
 * <p>Повторная доставка события возможна: потребители обязаны узнавать повтор
 * по стабильному {@link #eventId} и не обрабатывать событие второй раз (раздел 7).</p>
 *
 * @param eventId       стабильный идентификатор события — по нему потребители
 *                      узнают повторную доставку
 * @param userId        владелец метрик, одновременно key сообщения в Kafka
 * @param schemaVersion версия схемы полезной нагрузки (см. {@link SchemaVersions})
 * @param acceptedAt    момент принятия пакета MetricService (UTC)
 * @param requestId     идентификатор запроса, проходит через все сервисы
 *                      для сквозного поиска по логам
 * @param points        принятые точки измерений
 */
public record MetricEvent(
        String eventId,
        String userId,
        String schemaVersion,
        Instant acceptedAt,
        String requestId,
        List<MetricPoint> points) {
}
