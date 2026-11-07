package com.pulse.metric.service;

import com.pulse.contracts.dto.RegisterResponse;
import com.pulse.metric.exception.EmailAlreadyRegisteredException;
import com.pulse.metric.repository.UserRepository;
import java.util.UUID;
import java.util.regex.Pattern;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

/**
 * Регистрация (FR-1) и проверка учётных данных. Пароль при регистрации проверяется
 * на минимальную длину и хранится BCrypt-хешем — тот же кред используется для
 * Basic-аутентификации read API (Этап 5). Выборки для чтения — в {@link UserReadService}.
 */
@Service
public class UserService {

    private static final Pattern EMAIL_PATTERN =
            Pattern.compile("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");

    private static final int MIN_PASSWORD_LENGTH = 8;

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;

    public UserService(UserRepository userRepository, PasswordEncoder passwordEncoder) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
    }

    public RegisterResponse register(String email, String password) {
        if (email == null || !EMAIL_PATTERN.matcher(email).matches()) {
            throw new IllegalArgumentException("Некорректный email");
        }
        if (password == null || password.length() < MIN_PASSWORD_LENGTH) {
            throw new IllegalArgumentException(
                    "Пароль должен содержать не менее " + MIN_PASSWORD_LENGTH + " символов");
        }
        String normalizedEmail = email.toLowerCase();
        UUID userId = UUID.randomUUID();
        try {
            userRepository.insert(userId, normalizedEmail, passwordEncoder.encode(password));
        } catch (DataIntegrityViolationException exception) {
            throw new EmailAlreadyRegisteredException();
        }
        return new RegisterResponse(userId.toString());
    }

    public boolean userExists(UUID userId) {
        return userRepository.exists(userId);
    }

    /** Проверка пароля пользователя (для Basic-фильтра read-эндпоинтов). */
    public boolean passwordMatches(UUID userId, String rawPassword) {
        return userRepository.findPasswordHash(userId)
                .map(hash -> passwordEncoder.matches(rawPassword, hash))
                .orElse(false);
    }
}
