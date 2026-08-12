package com.sfkg.timeseries.service.impl;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.client.IngestBufferPool;
import com.sfkg.timeseries.client.TimeseriesCoreGrpcClient;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.dto.HistoryDataQueryRequest;
import com.sfkg.timeseries.dto.TimeseriesDataSaveRequest;
import com.sfkg.timeseries.entity.TimeseriesDataPoint;
import com.sfkg.timeseries.monitor.IngestThroughputMonitor;
import com.sfkg.timeseries.service.TimeseriesDataService;
import com.sfkg.timeseries.vo.HistoryDataVO;

@Service
public class TimeseriesDataServiceImpl implements TimeseriesDataService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeseriesDataServiceImpl.class);

    private final TimeseriesMemoryCache memoryCache;
    private final TimeseriesCoreGrpcClient coreGrpcClient;
    private final IngestBufferPool ingestBufferPool;
    private final IngestThroughputMonitor throughputMonitor;

    public TimeseriesDataServiceImpl(
            TimeseriesMemoryCache memoryCache,
            TimeseriesCoreGrpcClient coreGrpcClient,
            IngestBufferPool ingestBufferPool,
            IngestThroughputMonitor throughputMonitor) {
        this.memoryCache = memoryCache;
        this.coreGrpcClient = coreGrpcClient;
        this.ingestBufferPool = ingestBufferPool;
        this.throughputMonitor = throughputMonitor;
    }

    @Override
    public String saveTimeseriesData(TimeseriesDataSaveRequest request) {
        if (request == null || request.getPoints() == null || request.getPoints().isEmpty()) {
            throw new BusinessException("ingest points are required");
        }

        // ── File / cache writes removed from ingest hot path ──────────
        // (data is forwarded to Core via the buffer-pool; no local persistence for timeseries points)
        // List<TimeseriesDataPoint> localPoints = convertToDataPoints(request);
        // dataFileMapper.appendDataPoints(localPoints);
        // memoryCache.putTimeseriesDataPoints(localPoints);

        // Route each point through the hash-partitioned buffer pool
        int pointCount = request.getPoints().size();
        for (TimeseriesDataSaveRequest.IngestPointDTO point : request.getPoints()) {
            int partition = ingestBufferPool.partition(point.getSequenceId());
            ingestBufferPool.offer(point, partition);
        }
        throughputMonitor.recordReceived(pointCount);

        return String.valueOf(pointCount);
    }

    @Override
    public HistoryDataVO queryHistoryData(HistoryDataQueryRequest request) {
        validateHistoryQuery(request);
        return coreGrpcClient.queryHistoryData(request);
    }

    @Override
    public Map<String, Object> queryHistoryOverview(HistoryDataQueryRequest request) {
        return coreGrpcClient.queryHistoryOverview(request);
    }

    @Override
    public Map<String, Object> queryWindowData(HistoryDataQueryRequest request) {
        return coreGrpcClient.queryWindowData(request);
    }

    @Override
    public void validateHistoryQuery(HistoryDataQueryRequest request) {
        if (request != null
                && request.getStartTime() != null
                && request.getEndTime() != null
                && request.getStartTime().isAfter(request.getEndTime())) {
            throw new BusinessException("history query startTime must be before endTime");
        }
    }

    @Override
    public HistoryDataVO queryHistoryFromTdengine(HistoryDataQueryRequest request) {
        return new HistoryDataVO();
    }

    @Override
    public HistoryDataVO queryHistoryFromCoreGrpc(HistoryDataQueryRequest request) {
        return new HistoryDataVO();
    }

    @Override
    public HistoryDataVO convertHistoryData(Object rawData) {
        return new HistoryDataVO();
    }

    private List<TimeseriesDataPoint> convertToDataPoints(TimeseriesDataSaveRequest request) {
        List<TimeseriesDataPoint> points = new ArrayList<>();
        for (TimeseriesDataSaveRequest.IngestPointDTO p : request.getPoints()) {
            TimeseriesDataPoint dp = new TimeseriesDataPoint();
            dp.setSequenceId(p.getSequenceId());
            if (p.getTime() != null) {
                dp.setTimestamp(LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(p.getTime()), ZoneId.systemDefault()));
            }
            if (p.getDoubleValue() != null) {
                dp.setValue(BigDecimal.valueOf(p.getDoubleValue()));
            } else if (p.getInt64Value() != null) {
                dp.setValue(BigDecimal.valueOf(p.getInt64Value()));
            } else {
                dp.setValue(BigDecimal.ZERO);
            }
            points.add(dp);
        }
        return points;
    }
}
