package com.sfkg.timeseries.grpc.server;

import com.sfkg.timeseries.entity.TimeseriesEvent;
import com.sfkg.timeseries.grpc.EventMessage;
import com.sfkg.timeseries.grpc.SyncResponse;
import com.sfkg.timeseries.grpc.TimeseriesEventReceiverServiceGrpc;
import com.sfkg.timeseries.service.TimeseriesEventService;
import io.grpc.stub.StreamObserver;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class EventReceiverGrpcService extends TimeseriesEventReceiverServiceGrpc.TimeseriesEventReceiverServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(EventReceiverGrpcService.class);
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    private final TimeseriesEventService eventService;

    public EventReceiverGrpcService(TimeseriesEventService eventService) {
        this.eventService = eventService;
    }

    @Override
    public void receiveEvent(EventMessage request, StreamObserver<SyncResponse> responseObserver) {
        try {
            LOG.info("gRPC receiveEvent: eventId={}, eventName={}, eventType={}, eventSource={}",
                    request.getEventId(), request.getEventName(),
                    request.getEventType(), request.getEventSource());

            TimeseriesEvent entity = toEntity(request);
            eventService.saveEventEntity(entity);

            SyncResponse response = SyncResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("event received successfully")
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            LOG.info("gRPC receiveEvent success: eventId={}", entity.getEventId());
        } catch (Exception e) {
            LOG.error("gRPC receiveEvent failed: eventId={}", request.getEventId(), e);
            SyncResponse response = SyncResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("event receive failed: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    private TimeseriesEvent toEntity(EventMessage msg) {
        TimeseriesEvent entity = new TimeseriesEvent();
        entity.setEventId(!msg.getEventId().isEmpty() ? msg.getEventId() : null);
        entity.setEventName(emptyToNull(msg.getEventName()));
        entity.setEventType(emptyToNull(msg.getEventType()));
        entity.setEventSource(emptyToNull(msg.getEventSource()));
        entity.setRelatedSequences(msg.getRelatedSequencesCount() > 0
                ? List.copyOf(msg.getRelatedSequencesList()) : null);
        entity.setRelatedRules(msg.getRelatedRulesCount() > 0
                ? List.copyOf(msg.getRelatedRulesList()) : null);
        entity.setEventDescription(emptyToNull(msg.getEventDescription()));
        entity.setEventLevel(emptyToNull(msg.getEventLevel()));
        entity.setEventTime(parseDateTime(msg.getEventTime()));
        entity.setConfirmStatus(emptyToNull(msg.getConfirmStatus()));
        entity.setHandleStatus(emptyToNull(msg.getHandleStatus()));
        entity.setDiagnosisResult(emptyToNull(msg.getDiagnosisResult()));
        entity.setDiagnosisBasis(emptyToNull(msg.getDiagnosisBasis()));
        entity.setDisposalResult(emptyToNull(msg.getDisposalResult()));
        entity.setCreateTime(parseDateTime(msg.getCreateTime()));
        entity.setUpdateTime(parseDateTime(msg.getUpdateTime()));
        entity.setCreateUser(emptyToNull(msg.getCreateUser()));
        entity.setUpdateUser(emptyToNull(msg.getUpdateUser()));
        return entity;
    }

    private String emptyToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private LocalDateTime parseDateTime(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return LocalDateTime.parse(value, ISO_FORMATTER);
        } catch (DateTimeParseException e) {
            LOG.warn("Failed to parse datetime: {}", value, e);
            return null;
        }
    }
}
