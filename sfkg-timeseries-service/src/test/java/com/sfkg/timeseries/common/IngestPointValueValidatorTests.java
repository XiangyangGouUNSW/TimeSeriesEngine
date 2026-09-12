package com.sfkg.timeseries.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.sfkg.timeseries.dto.TimeseriesDataSaveRequest.IngestPointDTO;
import org.junit.jupiter.api.Test;

class IngestPointValueValidatorTests {

    @Test
    void acceptsExactlyOneValueField() {
        IngestPointDTO point = new IngestPointDTO();
        point.setDoubleValue(12.5);

        assertNull(IngestPointValueValidator.validationError(point));
    }

    @Test
    void rejectsPointWithoutValue() {
        String error = IngestPointValueValidator.validationError(new IngestPointDTO());

        assertEquals(
                "ingest point must provide exactly one value field: doubleValue, int64Value, boolValue, or stringValue",
                error);
    }

    @Test
    void rejectsPointWithMultipleValues() {
        IngestPointDTO point = new IngestPointDTO();
        point.setDoubleValue(12.5);
        point.setInt64Value(12L);

        assertEquals(
                "ingest point must provide only one value field: doubleValue, int64Value, boolValue, or stringValue",
                IngestPointValueValidator.validationError(point));
    }
}
