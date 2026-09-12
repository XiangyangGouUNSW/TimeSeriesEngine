package com.sfkg.timeseries.service.impl;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.List;

import com.sfkg.timeseries.client.IngestBufferPool;
import com.sfkg.timeseries.client.TimeseriesCoreGrpcClient;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.dto.TimeseriesDataSaveRequest;
import com.sfkg.timeseries.monitor.IngestThroughputMonitor;
import org.junit.jupiter.api.Test;

class TimeseriesDataServiceImplTests {

    private final TimeseriesCoreGrpcClient coreGrpcClient = mock(TimeseriesCoreGrpcClient.class);
    private final IngestBufferPool ingestBufferPool = mock(IngestBufferPool.class);
    private final IngestThroughputMonitor throughputMonitor = mock(IngestThroughputMonitor.class);
    private final TimeseriesDataServiceImpl service = new TimeseriesDataServiceImpl(
            coreGrpcClient, ingestBufferPool, throughputMonitor);

    @Test
    void rejectsWholeBatchBeforeBufferingWhenAnyPointHasNoValue() {
        TimeseriesDataSaveRequest.IngestPointDTO validPoint = new TimeseriesDataSaveRequest.IngestPointDTO();
        validPoint.setDoubleValue(12.5);
        TimeseriesDataSaveRequest.IngestPointDTO missingValuePoint = new TimeseriesDataSaveRequest.IngestPointDTO();
        TimeseriesDataSaveRequest request = requestWith(validPoint, missingValuePoint);

        BusinessException exception = assertThrows(
                BusinessException.class,
                () -> service.saveTimeseriesData(request));

        org.junit.jupiter.api.Assertions.assertTrue(exception.getMessage().contains("index 1"));
        verifyNoInteractions(ingestBufferPool, throughputMonitor, coreGrpcClient);
    }

    @Test
    void rejectsPointWithMoreThanOneValueType() {
        TimeseriesDataSaveRequest.IngestPointDTO point = new TimeseriesDataSaveRequest.IngestPointDTO();
        point.setDoubleValue(12.5);
        point.setInt64Value(12L);

        assertThrows(BusinessException.class, () -> service.saveTimeseriesData(requestWith(point)));
        verifyNoInteractions(ingestBufferPool, throughputMonitor, coreGrpcClient);
    }

    private TimeseriesDataSaveRequest requestWith(TimeseriesDataSaveRequest.IngestPointDTO... points) {
        TimeseriesDataSaveRequest request = new TimeseriesDataSaveRequest();
        request.setProjectId("project-a");
        request.setPoints(List.of(points));
        return request;
    }
}
