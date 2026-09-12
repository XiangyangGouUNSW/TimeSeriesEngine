package com.sfkg.timeseries.common;

import com.sfkg.timeseries.dto.TimeseriesDataSaveRequest.IngestPointDTO;

/** Validates the oneof-style value fields used by REST ingest requests. */
public final class IngestPointValueValidator {

    private IngestPointValueValidator() {
    }

    /**
     * Returns an error message when the point cannot be encoded as the gRPC
     * {@code TimeseriesValue} oneof; otherwise returns {@code null}.
     */
    public static String validationError(IngestPointDTO point) {
        if (point == null) {
            return "ingest point must not be null";
        }

        int valueCount = 0;
        if (point.getDoubleValue() != null) valueCount++;
        if (point.getInt64Value() != null) valueCount++;
        if (point.getBoolValue() != null) valueCount++;
        if (point.getStringValue() != null) valueCount++;

        if (valueCount == 0) {
            return "ingest point must provide exactly one value field: doubleValue, int64Value, boolValue, or stringValue";
        }
        if (valueCount > 1) {
            return "ingest point must provide only one value field: doubleValue, int64Value, boolValue, or stringValue";
        }
        return null;
    }
}
