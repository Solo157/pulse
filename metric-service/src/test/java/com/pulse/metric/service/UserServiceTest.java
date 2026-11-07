package com.pulse.metric.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.description;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.pulse.contracts.dto.RegisterResponse;
import com.pulse.metric.exception.EmailAlreadyRegisteredException;
import com.pulse.metric.repository.UserRepository;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.security.crypto.password.PasswordEncoder;

/**
 * Регистрация и проверка учётных данных (изоляция: замоканные репозиторий и encoder,
 * без БД и настоящего BCrypt).
 */
@ExtendWith(MockitoExtension.class)
class UserServiceTest {

    private static final UUID USER = UUID.fromString("33333333-3333-3333-3333-333333333333");

    @Mock
    private UserRepository userRepository;

    @Mock
    private PasswordEncoder passwordEncoder;

    @InjectMocks
    private UserService userService;

    @Test
    void shouldThrowIllegalArgument_whenPasswordShorterThanMinimum() {
        assertThatThrownBy(() -> userService.register("ok@pulse.dev", "short"))
                .as("пароль короче 8 символов отклоняется")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldHashPasswordAndReturnUserId_whenRegisterValid() {
        // Arrange
        when(passwordEncoder.encode("secret-password")).thenReturn("bcrypt-hash");

        // Act
        RegisterResponse response = userService.register("Ok@Pulse.dev", "secret-password");

        // Assert
        assertThat(UUID.fromString(response.userId())).as("userId — валидный UUID").isNotNull();
        verify(userRepository, description("пароль сохраняется только хешем, email в нижнем регистре"))
                .insert(any(UUID.class), eq("ok@pulse.dev"), eq("bcrypt-hash"));
    }

    @Test
    void shouldThrowConflict_whenEmailAlreadyTaken() {
        // Arrange
        when(userRepository.insert(any(), any(), any()))
                .thenThrow(new DataIntegrityViolationException("duplicate key"));

        assertThatThrownBy(() -> userService.register("taken@pulse.dev", "secret-password"))
                .as("повторный email — конфликт")
                .isInstanceOf(EmailAlreadyRegisteredException.class);
    }

    @Test
    void shouldReturnFalse_whenUserHasNoPasswordHash() {
        // Arrange
        when(userRepository.findPasswordHash(USER)).thenReturn(Optional.empty());

        // Act
        boolean matches = userService.passwordMatches(USER, "secret-password");

        // Assert
        assertThat(matches).as("нет хеша — не аутентифицирован").isFalse();
    }

    @Test
    void shouldReturnFalse_whenPasswordDoesNotMatchHash() {
        // Arrange
        when(userRepository.findPasswordHash(USER)).thenReturn(Optional.of("bcrypt-hash"));
        when(passwordEncoder.matches("wrong-password", "bcrypt-hash")).thenReturn(false);

        // Act
        boolean matches = userService.passwordMatches(USER, "wrong-password");

        // Assert
        assertThat(matches).as("неверный пароль отклоняется").isFalse();
    }
}
