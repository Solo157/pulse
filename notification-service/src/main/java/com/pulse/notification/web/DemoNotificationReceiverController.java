package com.pulse.notification.web;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Демонстрационный приёмный канал уведомлений: имитирует внешнюю систему доставки
 * (email-шлюз). Реальный канал подключается только после согласования (FR-5);
 * адрес настраивается через {@code pulse.delivery-url}.
 */
@RestController
public class DemoNotificationReceiverController {

    private static final Logger log = LoggerFactory.getLogger(DemoNotificationReceiverController.class);

    @PostMapping("/internal/demo-receiver")
    public ResponseEntity<Map<String, Object>> receive(@RequestBody Map<String, Object> notification) {
        log.info("ДЕМО-канал принял уведомление: destination={}, text={}",
                notification.get("destination"), notification.get("text"));
        return ResponseEntity.ok(Map.of("accepted", true,
                "notificationId", notification.getOrDefault("notificationId", "")));
    }
}
