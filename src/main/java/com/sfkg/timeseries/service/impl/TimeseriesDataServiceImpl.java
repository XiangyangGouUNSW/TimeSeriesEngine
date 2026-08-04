package com.sfkg.timeseries.service.impl;

import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.client.TimeseriesCoreGrpcClient;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.dto.HistoryDataQueryRequest;
import com.sfkg.timeseries.dto.SyncResult;
import com.sfkg.timeseries.dto.TimeseriesDataSaveRequest;
import com.sfkg.timeseries.entity.TimeseriesDataPoint;
import com.sfkg.timeseries.mapper.TimeseriesDataFileMapper;
import com.sfkg.timeseries.service.TimeseriesDataService;
import com.sfkg.timeseries.vo.HistoryDataVO;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class TimeseriesDataServiceImpl implements TimeseriesDataService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeseriesDataServiceImpl.class);

    private final TimeseriesDataFileMapper dataFileMapper;
    private final TimeseriesMemoryCache memoryCache;
    private final TimeseriesCoreGrpcClient coreGrpcClient;

    public TimeseriesDataServiceImpl(
            TimeseriesDataFileMapper dataFileMapper,
            TimeseriesMemoryCache memoryCache,
            TimeseriesCoreGrpcClient coreGrpcClient) {
        this.dataFileMapper = dataFileMapper;
        this.memoryCache = memoryCache;
        this.coreGrpcClient = coreGrpcClient;
    }

    @Override
    public Integer saveTimeseriesData(TimeseriesDataSaveRequest request) {
        List<TimeseriesDataPoint> points = convertSaveRequest(request);
        dataFileMapper.appendDataPoints(points);
        memoryCache.putTimeseriesDataPoints(points);
        SyncResult syncResult = coreGrpcClient.syncTimeseriesData(points);
        if (!syncResult.isSuccess()) {
            LOGGER.warn("timeseries core grpc sync skipped or failed: {}", syncResult.getMessage());
        }
        return points.size();
    }

    @Override
    public HistoryDataVO queryHistoryData(HistoryDataQueryRequest request) {
        validateHistoryQuery(request);
        List<TimeseriesDataPoint> filePoints = dataFileMapper.selectByCondition(request);
        memoryCache.replaceTimeseriesDataPoints(
                request == null ? null : request.getSequenceId(),
                filePoints);

        HistoryDataVO vo = new HistoryDataVO();
        if (request != null) {
            vo.setSequenceId(request.getSequenceId());
        }
        vo.setPoints(memoryCache.listTimeseriesDataPoints(request));
        return vo;
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

    private List<TimeseriesDataPoint> convertSaveRequest(TimeseriesDataSaveRequest request) {
        if (request == null || request.getSequenceId() == null) {
            throw new BusinessException("sequenceId is required");
        }

        List<TimeseriesDataPoint> points = new ArrayList<>();
        if (request.getPoints() != null && !request.getPoints().isEmpty()) {
            for (Map.Entry<String, BigDecimal> entry : request.getPoints().entrySet()) {
                points.add(createPoint(request.getSequenceId(), parseTimestamp(entry.getKey()), entry.getValue()));
            }
        }
        if (request.getTimestamp() != null || request.getValue() != null) {
            points.add(createPoint(request.getSequenceId(), request.getTimestamp(), request.getValue()));
        }
        if (points.isEmpty()) {
            throw new BusinessException("timeseries data points are required");
        }
        return points;
    }

    private TimeseriesDataPoint createPoint(Integer sequenceId, LocalDateTime timestamp, BigDecimal value) {
        if (timestamp == null) {
            throw new BusinessException("timestamp is required");
        }
        if (value == null) {
            throw new BusinessException("value is required");
        }
        TimeseriesDataPoint point = new TimeseriesDataPoint();
        point.setSequenceId(sequenceId);
        point.setTimestamp(timestamp);
        point.setValue(value);
        return point;
    }

    private LocalDateTime parseTimestamp(String timestamp) {
        if (timestamp == null || timestamp.isBlank()) {
            throw new BusinessException("timestamp is required");
        }
        try {
            return LocalDateTime.parse(timestamp);
        } catch (DateTimeParseException exception) {
            throw new BusinessException("timestamp format must be yyyy-MM-ddTHH:mm:ss");
        }
    }
}
