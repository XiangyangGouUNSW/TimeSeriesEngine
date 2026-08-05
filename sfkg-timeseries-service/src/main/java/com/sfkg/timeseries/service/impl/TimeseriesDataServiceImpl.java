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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
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
    public String saveTimeseriesData(TimeseriesDataSaveRequest request) {
        if (request == null || request.getPoints() == null || request.getPoints().isEmpty()) {
            throw new BusinessException("ingest points are required");
        }
        // Persist locally as TimeseriesDataPoint entries
        List<TimeseriesDataPoint> localPoints = convertToDataPoints(request);
        dataFileMapper.appendDataPoints(localPoints);
        memoryCache.putTimeseriesDataPoints(localPoints);

        // Forward to Core engine via gRPC ingestData
        SyncResult syncResult = coreGrpcClient.ingestData(request);
        if (!syncResult.isSuccess()) {
            LOGGER.warn("timeseries core grpc ingest skipped or failed: {}", syncResult.getMessage());
        }
        return String.valueOf(request.getPoints().size());
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
