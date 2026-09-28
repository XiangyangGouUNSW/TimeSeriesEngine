package com.sfkg.timeseries.service.impl;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.sfkg.timeseries.client.IngestBufferPool;
import com.sfkg.timeseries.client.TimeseriesCoreGrpcClient;
import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.common.IngestPointValueValidator;
import com.sfkg.timeseries.common.ProjectIdValidator;
import com.sfkg.timeseries.dto.HistoryDataQueryRequest;
import com.sfkg.timeseries.dto.TimeseriesDataSaveRequest;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.monitor.IngestThroughputMonitor;
import com.sfkg.timeseries.service.TimeseriesDataService;
import com.sfkg.timeseries.vo.HistoryDataVO;

@Service
public class TimeseriesDataServiceImpl implements TimeseriesDataService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeseriesDataServiceImpl.class);

    private final TimeseriesCoreGrpcClient coreGrpcClient;
    private final IngestBufferPool ingestBufferPool;
    private final IngestThroughputMonitor throughputMonitor;
    private final TimeseriesMemoryCache memoryCache;
    private final TimeseriesCacheManager cacheManager;

    public TimeseriesDataServiceImpl(
            TimeseriesCoreGrpcClient coreGrpcClient,
            IngestBufferPool ingestBufferPool,
            IngestThroughputMonitor throughputMonitor,
            TimeseriesMemoryCache memoryCache,
            TimeseriesCacheManager cacheManager) {
        this.coreGrpcClient = coreGrpcClient;
        this.ingestBufferPool = ingestBufferPool;
        this.throughputMonitor = throughputMonitor;
        this.memoryCache = memoryCache;
        this.cacheManager = cacheManager;
    }

    @Override
    public String saveTimeseriesData(TimeseriesDataSaveRequest request) {
        if (request == null || request.getPoints() == null || request.getPoints().isEmpty()) {
            throw new BusinessException("ingest points are required");
        }
        String projectId = ProjectIdValidator.require(request.getProjectId());
        request.setProjectId(projectId);
        cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);

        // Validate the whole batch before offering any point, so a malformed
        // value cannot become a partial write or a fabricated 0.0 observation.
        int pointCount = request.getPoints().size();
        for (int index = 0; index < pointCount; index++) {
            TimeseriesDataSaveRequest.IngestPointDTO point = request.getPoints().get(index);
            String valueError = IngestPointValueValidator.validationError(point);
            if (valueError != null) {
                throw new BusinessException("invalid ingest point at index " + index + ": " + valueError);
            }
            if (point.getTime() == null || point.getTime() <= 0) {
                throw new BusinessException("ingest point time must be positive at index " + index);
            }
            if (point.getSequenceId() == null || point.getSequenceId().isBlank()) {
                throw new BusinessException("ingest point sequenceId must not be empty at index " + index);
            }
            if (point.getProjectId() == null || point.getProjectId().isBlank()) {
                point.setProjectId(projectId);
            } else if (!projectId.equals(ProjectIdValidator.require(point.getProjectId()))) {
                throw new BusinessException("all ingest points must belong to projectId: " + projectId);
            } else {
                point.setProjectId(projectId);
            }
            validatePointDataType(projectId, point, index);
        }

        // Route only fully validated points through the hash-partitioned buffer pool.
        for (TimeseriesDataSaveRequest.IngestPointDTO point : request.getPoints()) {
            int partition = ingestBufferPool.partition(point.getProjectId(), point.getSequenceId());
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
        validateHistoryQuery(request);
        return coreGrpcClient.queryHistoryOverview(request);
    }

    @Override
    public Map<String, Object> queryWindowData(HistoryDataQueryRequest request) {
        validateHistoryQuery(request);
        return coreGrpcClient.queryWindowData(request);
    }

    @Override
    public void validateHistoryQuery(HistoryDataQueryRequest request) {
        if (request == null) {
            throw new BusinessException("history query request must not be null");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        boolean hasSingle = request.getSequenceId() != null && !request.getSequenceId().isBlank();
        boolean hasMultiple = request.getSequenceIds() != null && request.getSequenceIds().stream()
                .anyMatch(id -> id != null && !id.isBlank());
        if (!hasSingle && !hasMultiple) {
            throw new BusinessException("history query requires sequenceId or sequenceIds");
        }
        if (request.getStartTime() != null && request.getEndTime() != null
                && request.getStartTime().isAfter(request.getEndTime())) {
            throw new BusinessException("history query startTime must be before endTime");
        }
        if (request.getGranularity() != null && request.getGranularity() <= 0) {
            throw new BusinessException("history query granularity must be positive");
        }
    }

    private void validatePointDataType(String projectId,
            TimeseriesDataSaveRequest.IngestPointDTO point, int index) {
        TimeseriesInstanceConfig instance = memoryCache.getInstanceBySequenceId(projectId, point.getSequenceId());
        if (instance == null) {
            throw new BusinessException("ingest point sequence not found in project at index " + index
                    + ": " + point.getSequenceId());
        }
        String dataType = instance.getDataType() == null ? "" : instance.getDataType().trim().toLowerCase();
        boolean matches = switch (dataType) {
            case "double" -> point.getDoubleValue() != null;
            case "int64" -> point.getInt64Value() != null;
            case "bool" -> point.getBoolValue() != null;
            case "string" -> point.getStringValue() != null;
            default -> false;
        };
        if (!matches) {
            throw new BusinessException("ingest point value type does not match sequence dataType at index "
                    + index + ": " + point.getSequenceId());
        }
    }
}
