package com.pulse.contracts.dto;

/**
 * Результат успешной регистрации (FR-1).
 * Клиент использует {@code userId} во всех последующих запросах;
 * API-ключ в ДЕМО не используется.
 */
public record RegisterResponse(String userId) {
}
