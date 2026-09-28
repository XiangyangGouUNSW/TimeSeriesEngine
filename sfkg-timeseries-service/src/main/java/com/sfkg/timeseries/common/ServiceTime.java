package com.sfkg.timeseries.common;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

/** Shared UTC convention for epoch-millisecond conversions at service boundaries. */
public final class ServiceTime {

    public static final ZoneId ZONE_ID = ZoneOffset.UTC;

    private ServiceTime() {
    }

    /** Epoch milliseconds for a service-local timestamp; {@code null} becomes {@code 0}. */
    public static long toEpochMillis(LocalDateTime value) {
        return value == null ? 0L : value.atZone(ZONE_ID).toInstant().toEpochMilli();
    }
}
