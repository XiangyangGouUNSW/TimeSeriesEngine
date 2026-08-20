package com.sfkg.timeseries.grpc.server;

import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.entity.TimeseriesForecastResult;
import com.sfkg.timeseries.grpc.AnalysisResultReceiverServiceGrpc;
import com.sfkg.timeseries.grpc.ForecastResultMessage;
import com.google.protobuf.Empty;
import com.sfkg.timeseries.mapper.TimeseriesForecastResultMapper;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class ForecastResultReceiverGrpcService
        extends AnalysisResultReceiverServiceGrpc.AnalysisResultReceiverServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(ForecastResultReceiverGrpcService.class);

    private final TimeseriesForecastResultMapper resultMapper;
    private final TimeseriesMemoryCache memoryCache;
    private final TimeseriesCacheManager cacheManager;

    public ForecastResultReceiverGrpcService(
            TimeseriesForecastResultMapper resultMapper,
            TimeseriesMemoryCache memoryCache,
            TimeseriesCacheManager cacheManager) {
        this.resultMapper = resultMapper;
        this.memoryCache = memoryCache;
        this.cacheManager = cacheManager;
    }

    @Override
    public void receiveForecastResult(ForecastResultMessage request,
                                      StreamObserver<Empty> responseObserver) {
        try {
            LOG.info("gRPC receiveForecastResult: taskId={}, runId={}, status={}, seqs={}",
                    request.getTaskId(), request.getRunId(), request.getStatus(),
                    request.getSequenceIdsList());

            cacheManager.ensureTableLoaded(CachedTable.FORECAST_RESULT);

            TimeseriesForecastResult entity = toEntity(request);
            resultMapper.insert(entity);
            memoryCache.putForecastResult(entity);

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            LOG.info("gRPC receiveForecastResult success: resultId={}", entity.getResultId());
        } catch (Exception e) {
            LOG.error("gRPC receiveForecastResult failed", e);
            responseObserver.onError(e);
        }
    }

    private TimeseriesForecastResult toEntity(ForecastResultMessage msg) {
        TimeseriesForecastResult entity = new TimeseriesForecastResult();
        entity.setProjectId(emptyToNull(msg.getProjectId()));
        entity.setResultId("FR_" + msg.getTaskId() + "_" + msg.getRunId() + "_" + UUID.randomUUID().toString().substring(0, 8));
        entity.setTaskId(msg.getTaskId().isBlank() ? null : msg.getTaskId());
        entity.setRunId(msg.getRunId().isBlank() ? null : msg.getRunId());
        entity.setGeneratedAt(msg.getGeneratedAtMs() > 0
                ? LocalDateTime.ofInstant(Instant.ofEpochMilli(msg.getGeneratedAtMs()), ZoneId.systemDefault())
                : null);
        entity.setStatus(msg.getStatus() != null && msg.getStatus() != com.sfkg.timeseries.grpc.AnalysisStatus.ANALYSIS_STATUS_UNSPECIFIED
                ? msg.getStatus().name() : null);
        entity.setMessage(emptyToNull(msg.getMessage()));
        entity.setTimestampsMs(msg.getTimestampsMsCount() > 0
                ? new ArrayList<>(msg.getTimestampsMsList()) : null);
        entity.setSequenceIds(msg.getSequenceIdsCount() > 0
                ? List.copyOf(msg.getSequenceIdsList()) : null);
        entity.setValues(msg.getValuesCount() > 0
                ? new ArrayList<>(msg.getValuesList()) : null);
        entity.setReceivedTime(LocalDateTime.now());
        return entity;
    }

    private String emptyToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
