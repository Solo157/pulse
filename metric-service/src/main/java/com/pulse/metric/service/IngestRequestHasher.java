package com.pulse.metric.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulse.contracts.common.MetricPoint;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

/**
 * Отпечаток тела запроса ingest для контроля повторов идемпотентности:
 * повтор с тем же {@code Idempotency-Key}, но другим телом обязан получить 409,
 * а не previously-принятый ответ на другое тело.
 *
 * <p>Хешируется каноническая JSON-сериализация списка точек (порядок точек
 * значим — это часть запроса), SHA-256, hex.</p>
 */
public final class IngestRequestHasher {

    private IngestRequestHasher() {
    }

    public static String hash(ObjectMapper objectMapper, List<MetricPoint> points) {
        try {
            byte[] canonical = objectMapper.writeValueAsBytes(points);
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical));
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Не сериализуются точки для отпечатка", failure);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 отсутствует в JRE", impossible);
        }
    }

    /** Сравнение отпечатков; записи прежних этапов без хеша считаются несовпадающими. */
    public static boolean matches(String storedHash, String requestHash) {
        return storedHash != null && storedHash.equals(requestHash);
    }
}
