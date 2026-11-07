package com.pulse.notification.repository;

/**
 * Статус уведомления (FR-5). DELIVERING — доставка начата (claim сделан),
 * терминальные: SENT (канал принял) и FAILED (исчерпаны попытки).
 */
public enum NotificationStatus {
    DELIVERING, SENT, FAILED
}
