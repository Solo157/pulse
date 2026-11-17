package com.pulse.contracts.event;

/**
 * Версионирование схемы Kafka-событий (разделы 7, 9).
 * Увеличивается при обратно несовместимом изменении формата полезной нагрузки.
 */
public final class SchemaVersions {

    /** Текущая версия схемы событий Pulse. */
    public static final String CURRENT = "1";

    private SchemaVersions() {
    }
}
