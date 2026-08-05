package com.sfkg.timeseries.grpc.server;

import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.entity.TimeseriesAnomalyResult;
import com.sfkg.timeseries.grpc.AnalysisResultReceiverServiceGrpc;
import com.sfkg.timeseries.grpc.AnomalyResultMessage;
import com.google.protobuf.Empty;
import com.sfkg.timeseries.mapper.TimeseriesAnomalyResultMapper;
import com.sfkg.timeseries.service.TimeseriesAnomalyResultService;
import com.sfkg.timeseries.vo.AnomalyResultVO;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class AnomalyResultReceiverGrpcService
        extends AnalysisResultReceiverServiceGrpc.AnalysisResultReceiverServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(AnomalyResultReceiverGrpcService.class);

    private final TimeseriesAnomalyResultMapper resultMapper;
    private final TimeseriesMemoryCache memoryCache;
    private final TimeseriesCacheManager cacheManager;
    private final TimeseriesAnomalyResultService anomalyResultService;

    public AnomalyResultReceiverGrpcService(
            TimeseriesAnomalyResultMapper resultMapper,
            TimeseriesMemoryCache memoryCache,
            TimeseriesCacheManager cacheManager,
            TimeseriesAnomalyResultService anomalyResultService) {
        this.resultMapper = resultMapper;
        this.memoryCache = memoryCache;
        this.cacheManager = cacheManager;
        this.anomalyResultService = anomalyResultService;
    }

    @Override
    public void receiveAnomalyResult(AnomalyResultMessage request,
                                     StreamObserver<Empty> responseObserver) {
        try {
            LOG.info("gRPC receiveAnomalyResult: eventType={}, severity={}, source={}, seqs={}",
                    request.getEventType(), request.getSeverity(), request.getSource(),
                    request.getSequenceIdsList());

            cacheManager.ensureTableLoaded(CachedTable.ANOMALY_RESULT);

            TimeseriesAnomalyResult entity = toEntity(request);
            resultMapper.insert(entity);
            memoryCache.putAnomalyResult(entity);

            // create an event from this anomaly result
            AnomalyResultVO vo = new AnomalyResultVO();
            vo.setResultId(entity.getResultId());
            vo.setSequenceId(entity.getSequenceIds() != null && !entity.getSequenceIds().isEmpty()
                    ? String.join(",", entity.getSequenceIds()) : null);
            vo.setAnomalyLevel(entity.getSeverity());
            anomalyResultService.createAnomalyEvent(vo);

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            LOG.info("gRPC receiveAnomalyResult success: resultId={}", entity.getResultId());
        } catch (Exception e) {
            LOG.error("gRPC receiveAnomalyResult failed", e);
            responseObserver.onError(e);
        }
    }

    private TimeseriesAnomalyResult toEntity(AnomalyResultMessage msg) {
        TimeseriesAnomalyResult entity = new TimeseriesAnomalyResult();
        String source = msg.getSource().name();
        String seqs = String.join("_", msg.getSequenceIdsList());
        entity.setResultId(source + "_" + seqs + "_" + msg.getEventTimeMs());
        entity.setEventType(msg.getEventType().name());
        entity.setEventTime(msg.getEventTimeMs() > 0
                ? LocalDateTime.ofInstant(Instant.ofEpochMilli(msg.getEventTimeMs()), ZoneId.systemDefault())
                : null);
        entity.setSequenceIds(msg.getSequenceIdsCount() > 0
                ? List.copyOf(msg.getSequenceIdsList()) : null);
        entity.setValues(msg.getValuesCount() > 0
                ? new java.util.ArrayList<>(msg.getValuesList()) : null);
        entity.setSeverity(msg.getSeverity().name());
        entity.setSource(source);
        entity.setReceivedTime(LocalDateTime.now());
        return entity;
    }
}
