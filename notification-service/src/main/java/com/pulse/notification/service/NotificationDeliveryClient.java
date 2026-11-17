package com.pulse.notification.service;

import com.pulse.notification.config.NotificationProperties;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * HTTP-клиент демонстрационного канала доставки. Адрес ({@code pulse.delivery-url})
 * читается из Environment при каждой доставке: по умолчанию это собственный endpoint
 * сервиса {@code /internal/demo-receiver}; в тестах — адрес запущенного приложения
 * (порт {@code local.server.port} известен только после старта) либо заведомо
 * недоступный адрес для проверки ветки FAILED.
 */
@Component
public class NotificationDeliveryClient {

    private static final Logger log = LoggerFactory.getLogger(NotificationDeliveryClient.class);
    private static final ParameterizedTypeReference<Map<String, Object>> MAP_TYPE =
            new ParameterizedTypeReference<>() { };

    private final RestClient restClient;
    private final Environment environment;

    public NotificationDeliveryClient(NotificationProperties properties, Environment environment) {
        this.environment = environment;
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(properties.deliveryTimeout());
        factory.setReadTimeout(properties.deliveryTimeout());
        this.restClient = RestClient.builder().requestFactory(factory).build();
    }

    /** true — канал ответил 2xx; любой сетевой или HTTP-сбой — false. */
    public boolean deliver(Map<String, Object> payload) {
        try {
            restClient.post()
                    // Адрес читается при каждой доставке: в тестах он ссылается на
                    // local.server.port, который известен только после запуска веб-сервера.
                    .uri(environment.getRequiredProperty("pulse.delivery-url"))
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(payload)
                    .retrieve()
                    .toEntity(MAP_TYPE);
            return true;
        } catch (RuntimeException failure) {
            log.warn("Демонстрационная доставка не удалась: {}", failure.getMessage());
            return false;
        }
    }
}
