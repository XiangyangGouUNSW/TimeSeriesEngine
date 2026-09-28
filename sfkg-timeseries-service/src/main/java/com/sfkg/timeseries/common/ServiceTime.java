package com.sfkg.timeseries.common;

import java.time.ZoneId;
import java.time.ZoneOffset;

/** Shared UTC convention for epoch-millisecond conversions at service boundaries. */
public final class ServiceTime {

    public static final ZoneId ZONE_ID = ZoneOffset.UTC;

    private ServiceTime() {
    }
}
