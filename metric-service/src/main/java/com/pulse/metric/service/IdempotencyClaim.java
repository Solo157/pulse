package com.pulse.metric.service;

/**
 * Результат попытки заявить Idempotency-Key для пары (пользователь, заголовок).
 *
 * @param type           чем закончилась попытка
 * @param eventId        eventId завершённого приёма (только REPLAY)
 * @param acceptedCount  сколько точек было принято (только REPLAY)
 */
public record IdempotencyClaim(ClaimType type, String eventId, Integer acceptedCount) {

    public enum ClaimType {
        /** Ключ заявлен впервые — можно публиковать событие. */
        FRESH,
        /** Приём с этим ключом завершён — отдаём прежний ответ. */
        REPLAY_COMPLETED,
        /** Предыдущий приём с этим ключом ещё не завершён — повтор раньше времени. */
        CONFLICT_IN_PROGRESS
    }

    public static IdempotencyClaim fresh() {
        return new IdempotencyClaim(ClaimType.FRESH, null, null);
    }

    public static IdempotencyClaim replayCompleted(String eventId, int acceptedCount) {
        return new IdempotencyClaim(ClaimType.REPLAY_COMPLETED, eventId, acceptedCount);
    }

    public static IdempotencyClaim conflictInProgress() {
        return new IdempotencyClaim(ClaimType.CONFLICT_IN_PROGRESS, null, null);
    }
}
