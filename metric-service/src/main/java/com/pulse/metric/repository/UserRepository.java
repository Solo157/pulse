package com.pulse.metric.repository;

import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Пользователи (таблица metric.users) на JdbcTemplate.
 */
@Repository
public class UserRepository {

    private final JdbcTemplate jdbcTemplate;

    public UserRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Заводит пользователя.
     *
     * @return false, если email уже занят (нарушение UNIQUE)
     */
    public boolean insert(UUID userId, String email, String passwordHash) {
        int inserted = jdbcTemplate.update(
                "INSERT INTO metric.users (id, email, password_hash) VALUES (?, ?, ?)",
                userId, email, passwordHash);
        return inserted == 1;
    }

    public boolean exists(UUID userId) {
        return !jdbcTemplate.query("SELECT 1 FROM metric.users WHERE id = ?",
                        (resultSet, rowNumber) -> rowNumber, userId).isEmpty();
    }

    /**
     * Хеш пароля пользователя (BCrypt) для Basic-аутентификации read API (Et5).
     * Пустой результат — пользователя нет.
     */
    public Optional<String> findPasswordHash(UUID userId) {
        return jdbcTemplate.query(
                        "SELECT password_hash FROM metric.users WHERE id = ?",
                        (resultSet, rowNumber) -> resultSet.getString(1), userId)
                .stream().findFirst();
    }
}
