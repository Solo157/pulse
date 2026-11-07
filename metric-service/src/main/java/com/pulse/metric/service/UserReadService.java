package com.pulse.metric.service;

import com.pulse.contracts.dto.IncidentResponse;
import com.pulse.contracts.dto.MetricReading;
import com.pulse.metric.exception.DataUnavailableException;
import com.pulse.metric.repository.IncidentReadRepository;
import com.pulse.metric.repository.MetricHistoryRepository;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Service;

/**
 * Пользовательское чтение (FR-4, Этап 5): история метрик и инцидентов владельца.
 * Отдельный сервис: в {@code UserService} остаётся только регистрация и проверка учётных
 * данных (CRUD/аутентификация), здесь — выборки для read API.
 *
 * <p>Fail-fast зависимостей (находка F5/F7): каждая выборка выполняется в изолированном
 * пуле и принудительно обрывается {@link TimeLimiter} по таймауту — драйвер ClickHouse
 * игнорирует connect/socket параметры и при недоступном сервере держит соединение ~30 с.
 * Сбои (включая таймауты) копятся в {@link CircuitBreaker}: открытая цепь мгновенно
 * отвечает {@link DataUnavailableException} (503), half-open проверяет восстановление.
 * Выборка в отдельном пуле нужна затем, что аннотационный {@code @TimeLimiter} оборачивает
 * уже вычисленный future, а блокирующий JDBC-вызов к тому моменту отработал бы на потоке
 * запроса; здесь timeout реально обрывает ожидание вызывающей стороны.
 *
 * <p>Выборка может продолжить выполняться в пуле после обрыва (JDBC-сокет не прерывается),
 * но не влияет на ответ и не копит потоки: пул конечен, лишние задачи получают отказ. В
 * production-контуре потолок даёт и балансировщик (nginx proxy_read_timeout).
 */
@Service
public class UserReadService implements DisposableBean {

    private static final int POOL_SIZE = 8;
    private static final int QUEUE_SIZE = 32;

    private final MetricHistoryRepository metricHistoryRepository;
    private final IncidentReadRepository incidentReadRepository;
    private final ExecutorService queryExecutor;
    private final CircuitBreaker historyBreaker;
    private final TimeLimiter historyLimiter;
    private final CircuitBreaker incidentsBreaker;
    private final TimeLimiter incidentsLimiter;
    private final java.util.concurrent.ScheduledExecutorService timeoutScheduler;

    public UserReadService(MetricHistoryRepository metricHistoryRepository,
                           IncidentReadRepository incidentReadRepository,
                           CircuitBreakerRegistry circuitBreakerRegistry,
                           TimeLimiterRegistry timeLimiterRegistry) {
        this.metricHistoryRepository = metricHistoryRepository;
        this.incidentReadRepository = incidentReadRepository;
        this.queryExecutor = newQueryPool();
        this.timeoutScheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
                runnable -> {
                    Thread thread = new Thread(runnable, "read-timelimiter");
                    thread.setDaemon(true);
                    return thread;
                });
        this.historyBreaker = circuitBreakerRegistry.circuitBreaker("chRead");
        this.historyLimiter = timeLimiterRegistry.timeLimiter("chRead");
        this.incidentsBreaker = circuitBreakerRegistry.circuitBreaker("pgRead");
        this.incidentsLimiter = timeLimiterRegistry.timeLimiter("pgRead");
    }

    /**
     * История метрик из ClickHouse с ограничением выполнения и circuit breaker.
     *
     * @return future со списком точек; при недоступности/таймауте — FailedFuture с
     *         {@link DataUnavailableException} (HTTP 503)
     */
    public CompletableFuture<List<MetricReading>> getHistory(UUID userId, String metricName,
                                                             Instant from, Instant to,
                                                             int limit) {
        return guarded(historyBreaker, historyLimiter, () ->
                metricHistoryRepository.find(userId, metricName, from, to, limit));
    }

    /** Инциденты из PostgreSQL — тот же fast-fail паттерн (цепь {@code pgRead}). */
    public CompletableFuture<List<IncidentResponse>> getIncidents(UUID userId, Instant from,
                                                                  int limit) {
        return guarded(incidentsBreaker, incidentsLimiter, () ->
                incidentReadRepository.find(userId, from, limit));
    }

    /**
     * Цепочка guard'ов (внешний — breaker, внутренний — limiter): breaker обёрнут вокруг
     * ограниченно-по-времени выборки, поэтому и {@code TimeoutException}, и сетевой сбой
     * таймаут драйвера и открывают цепь; {@code executeCompletionStage} фиксирует
     * и асинхронные сбои future; любой отказ приводится к {@link DataUnavailableException}.
     */
    private <T> CompletableFuture<T> guarded(CircuitBreaker breaker, TimeLimiter limiter,
                                             Supplier<T> query) {
        try {
            Supplier<CompletableFuture<T>> queryTask =
                    () -> CompletableFuture.supplyAsync(query, queryExecutor);
            Supplier<CompletionStage<T>> guardedTask = limiter.decorateCompletionStage(
                    timeoutScheduler, queryTask);
            return breaker.<T>executeCompletionStage(guardedTask).toCompletableFuture()
                    .exceptionally(UserReadService::toUnavailable);
        } catch (CallNotPermittedException exception) {
            return CompletableFuture.failedFuture(new DataUnavailableException(
                    "Сервис чтения временно недоступен (цепь открыта)", exception));
        } catch (RuntimeException exception) {
            return CompletableFuture.failedFuture(new DataUnavailableException(
                    "Сервис чтения временно недоступен", exception));
        }
    }

    private static <T> T toUnavailable(Throwable cause) {
        Throwable root = cause instanceof CompletionException && cause.getCause() != null
                ? cause.getCause() : cause;
        if (root instanceof DataUnavailableException dataUnavailable) {
            throw dataUnavailable;
        }
        throw new DataUnavailableException("Хранилище недоступно или отвечает слишком долго",
                root);
    }

    private static ExecutorService newQueryPool() {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(POOL_SIZE, POOL_SIZE, 0L,
                TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(QUEUE_SIZE),
                runnable -> {
                    Thread thread = new Thread(runnable, "read-query");
                    thread.setDaemon(true);
                    return thread;
                });
        return pool;
    }

    @Override
    public void destroy() {
        timeoutScheduler.shutdownNow();
        queryExecutor.shutdownNow();
    }
}
