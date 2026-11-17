package com.pulse.contracts.dto;

/**
 * Ответ на успешный приём пакета метрик: HTTP 202 (FR-2, раздел 9).
 * Возвращается только после подтверждения записи брокером Kafka.
 *
 * @param eventId       идентификатор события, в которое развёрнут пакет (тот же при повторном Idempotency-Key)
 * @param userId        владелец метрик
 * @param acceptedCount количество принятых точек
 */
public record IngestResponse(String eventId, String userId, int acceptedCount) {
}
