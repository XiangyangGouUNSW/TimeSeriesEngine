package com.sfkg.timeseries.grpc.server;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.entity.TimeseriesConstraintResult;
import com.sfkg.timeseries.entity.TimeseriesEvent;
import com.sfkg.timeseries.grpc.ConstraintResultMessage;
import com.sfkg.timeseries.grpc.SyncResponse;
import com.sfkg.timeseries.grpc.TimeseriesConstraintResultReceiverServiceGrpc;
import com.sfkg.timeseries.mapper.TimeseriesConstraintResultMapper;
import com.sfkg.timeseries.mapper.TimeseriesEventMapper;

import io.grpc.stub.StreamObserver;

@Component
public class ConstraintResultReceiverGrpcService
        extends TimeseriesConstraintResultReceiverServiceGrpc.TimeseriesConstraintResultReceiverServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(ConstraintResultReceiverGrpcService.class);

    private final TimeseriesConstraintResultMapper resultMapper;
    private final TimeseriesEventMapper eventMapper;
    private final TimeseriesMemoryCache memoryCache;

    public ConstraintResultReceiverGrpcService(
            TimeseriesConstraintResultMapper resultMapper,
            TimeseriesEventMapper eventMapper,
            TimeseriesMemoryCache memoryCache) {
        this.resultMapper = resultMapper;
        this.eventMapper = eventMapper;
        this.memoryCache = memoryCache;
    }

    @Override
    public void receiveConstraintResult(ConstraintResultMessage request,
                                        StreamObserver<SyncResponse> responseObserver) {
        try {
            LOG.info("gRPC receiveConstraintResult: violatedIds={}, seqs={}, time={}",
                    request.getViolatedConstraintIdsList(), request.getSequenceIdsList(),
                    request.getCheckTimeMs());

            TimeseriesConstraintResult entity = toEntity(request);
            resultMapper.insert(entity);

            if (request.getViolatedConstraintIdsCount() > 0) {
                String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyMMddHHmmssSSS"));
                String cids = String.join("_", request.getViolatedConstraintIdsList());
                String eventId = "EVT_CONSTRAINT_" + cids + "_" + ts;
                memoryCache.computeEvent(eventId, existing -> {
                    if (existing != null) {
                        LOG.info("constraint event already exists, skip: eventId={}", eventId);
                        return existing;
                    }
                    TimeseriesEvent event = new TimeseriesEvent();
                    event.setEventId(eventId);
                    event.setEventName("constraint violated: " + cids);
                    event.setEventType("ANOMALY");
                    event.setEventSource("CONSTRAINT_CHECK");
                    event.setEventLevel("MEDIUM");
                    event.setEventTime(entity.getCheckTime());
                    event.setRelatedSequences(entity.getSequenceIds());
                    event.setRelatedRules(entity.getViolatedConstraintIds());
                    event.setEventDescription(
                            "violated constraints " + entity.getViolatedConstraintIds()
                            + " on sequences " + entity.getSequenceIds());
                    event.setConfirmStatus("PENDING");
                    event.setHandleStatus("UNHANDLED");
                    LocalDateTime now = LocalDateTime.now();
                    event.setCreateTime(now);
                    event.setUpdateTime(now);
                    eventMapper.insert(event);
                    return event;
                });
                LOG.info("constraint violation event created: eventId={}", eventId);
            }

            responseObserver.onNext(SyncResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error("gRPC receiveConstraintResult failed", e);
            responseObserver.onError(e);
        }
    }

    private TimeseriesConstraintResult toEntity(ConstraintResultMessage msg) {
        TimeseriesConstraintResult entity = new TimeseriesConstraintResult();
        entity.setResultId("CR_" + UUID.randomUUID().toString().substring(0, 8));
        entity.setCheckTime(msg.getCheckTimeMs() > 0
                ? LocalDateTime.ofInstant(Instant.ofEpochMilli(msg.getCheckTimeMs()), ZoneId.systemDefault())
                : LocalDateTime.now());
        entity.setViolatedConstraintIds(msg.getViolatedConstraintIdsCount() > 0
                ? List.copyOf(msg.getViolatedConstraintIdsList()) : List.of());
        entity.setSequenceIds(msg.getSequenceIdsCount() > 0
                ? List.copyOf(msg.getSequenceIdsList()) : null);
        entity.setReceivedTime(LocalDateTime.now());
        return entity;
    }

    private String emptyToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
