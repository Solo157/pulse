package com.pulse.processing.listener;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pulse.contracts.event.IncidentDetected;
import com.pulse.contracts.event.SchemaVersions;
import com.pulse.processing.state.IncidentRowRepository;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Группа metric-group сохраняет переходы инцидентов: открытие — строку в БД,
 * закрытие — по incidentKey; неразобранное сообщение остаётся для повтора
 * error-handler'ом (изоляция: замоканный репозиторий).
 */
@ExtendWith(MockitoExtension.class)
class IncidentPersistListenerTest {

    private static final UUID USER = UUID.fromString("88888888-8888-8888-8888-888888888888");
    private static final UUID EVENT = UUID.fromString("99999999-9999-9999-9999-999999999999");
    private static final Instant NOW = Instant.parse("2026-11-09T10:00:00Z");

    @Mock
    private IncidentRowRepository rowRepository;

    @Test
    void shouldOpenIncidentRow_whenIncidentOpened() {
        // Arrange
        IncidentPersistListener listener = new IncidentPersistListener(rowRepository, objectMapper());

        // Act
        listener.onIncidentDetected(payload(false));

        // Assert
        verify(rowRepository).open(EVENT, USER + ":rule-1", USER, "rule-1", "cpu", 90.0, 80.0, NOW);
    }

    @Test
    void shouldCloseIncidentRow_whenIncidentClosed() {
        // Arrange
        IncidentPersistListener listener = new IncidentPersistListener(rowRepository, objectMapper());

        // Act
        listener.onIncidentDetected(payload(true));

        // Assert
        verify(rowRepository).close(USER + ":rule-1", NOW);
    }

    @Test
    void shouldFailForRedelivery_whenPayloadUnreadable() {
        // Arrange
        IncidentPersistListener listener = new IncidentPersistListener(rowRepository, objectMapper());

        // Act & Assert
        assertThatThrownBy(() -> listener.onIncidentDetected("{битое"))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(rowRepository);
    }

    private static ObjectMapper objectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }

    private static String payload(boolean close) {
        try {
            return objectMapper().writeValueAsString(new IncidentDetected(EVENT.toString(),
                    USER.toString(), "rule-1", USER + ":rule-1", close, "cpu", 80.0, 90.0,
                    NOW, NOW.minusSeconds(2), SchemaVersions.CURRENT, "req-1"));
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }
}
