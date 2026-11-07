package com.pulse.contracts.dto;

/**
 * Тело запроса {@code POST /auth/register} (FR-1).
 * В ДЕМО пароль только валидируется и не сохраняется.
 */
public record RegisterRequest(String email, String password) {
}
