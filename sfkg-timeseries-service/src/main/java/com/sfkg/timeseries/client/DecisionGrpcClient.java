package com.sfkg.timeseries.client;

import com.sfkg.timeseries.config.GrpcClientProperties;
import com.sfkg.timeseries.dto.DecisionContext;
import com.sfkg.timeseries.grpc.AnalysisDecisionRequest;
import com.sfkg.timeseries.grpc.AnalysisDecisionResult;
import com.sfkg.timeseries.grpc.RequestMeta;
import com.sfkg.timeseries.grpc.TimeseriesAnalysisServiceGrpc;
import com.sfkg.timeseries.vo.DecisionSuggestionVO;
import com.sfkg.timeseries.vo.DiagnosisResultVO;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DecisionGrpcClient {

    private static final Logger LOG = LoggerFactory.getLogger(DecisionGrpcClient.class);
    private static final String SERVICE_NAME = "timeseries-analysis";

    private final GrpcClientProperties grpcClientProperties;
    private final GrpcChannelRegistry channelRegistry;

    public DecisionGrpcClient(GrpcClientProperties grpcClientProperties,
                              GrpcChannelRegistry channelRegistry) {
        this.grpcClientProperties = grpcClientProperties;
        this.channelRegistry = channelRegistry;
    }

    public DiagnosisResultVO generateDiagnosisResult(DecisionContext context) {
        String address = grpcClientProperties.getDecisionAddress();
        if (isBlank(address)) {
            LOG.info("[{}] generateDiagnosis skipped: address not configured", SERVICE_NAME);
            return new DiagnosisResultVO();
        }
        if (context == null) {
            return new DiagnosisResultVO();
        }
        AnalysisDecisionRequest req = buildRequest(context);
        LOG.info("[{}] -> GenerateDiagnosis eventId={} at {}", SERVICE_NAME, context.getEventId(), address);

        ManagedChannel channel = channelRegistry.getChannel(address);
        try {
            AnalysisDecisionResult resp = TimeseriesAnalysisServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .generateDiagnosis(req);
            LOG.info("[{}] <- GenerateDiagnosis eventId={} status={}", SERVICE_NAME,
                    resp.getEventId(), resp.getStatus());
            DiagnosisResultVO vo = new DiagnosisResultVO();
            vo.setEventId(resp.getEventId());
            vo.setDiagnosisResult(resp.getContent());
            vo.setDiagnosisBasis(resp.getMessage());
            return vo;
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] GenerateDiagnosis FAILED: {}", SERVICE_NAME, e.getStatus().getDescription());
            return new DiagnosisResultVO();
        }
    }

    public DecisionSuggestionVO generateDecisionSuggestion(DecisionContext context) {
        String address = grpcClientProperties.getDecisionAddress();
        if (isBlank(address)) {
            LOG.info("[{}] generateSuggestion skipped: address not configured", SERVICE_NAME);
            return new DecisionSuggestionVO();
        }
        if (context == null) {
            return new DecisionSuggestionVO();
        }
        AnalysisDecisionRequest req = buildRequest(context);
        LOG.info("[{}] -> GenerateSuggestion eventId={} at {}", SERVICE_NAME, context.getEventId(), address);

        ManagedChannel channel = channelRegistry.getChannel(address);
        try {
            AnalysisDecisionResult resp = TimeseriesAnalysisServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .generateSuggestion(req);
            LOG.info("[{}] <- GenerateSuggestion eventId={} status={}", SERVICE_NAME,
                    resp.getEventId(), resp.getStatus());
            DecisionSuggestionVO vo = new DecisionSuggestionVO();
            vo.setEventId(resp.getEventId());
            vo.setSuggestion(resp.getContent());
            return vo;
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] GenerateSuggestion FAILED: {}", SERVICE_NAME, e.getStatus().getDescription());
            return new DecisionSuggestionVO();
        }
    }

    // ── helpers ────────────────────────────────────────────────────────

    private AnalysisDecisionRequest buildRequest(DecisionContext context) {
        AnalysisDecisionRequest.Builder builder = AnalysisDecisionRequest.newBuilder()
                .setMeta(RequestMeta.newBuilder()
                        .setRequestId(UUID.randomUUID().toString())
                        .setSentAtMs(System.currentTimeMillis())
                        .build())
                .setEventId(nullToEmpty(context.getEventId()));
        if (context.getEventInfo() != null) {
            Object eventType = context.getEventInfo().get("eventType");
            if (eventType != null) builder.setEventType(eventType.toString());
            Object summary = context.getEventInfo().get("eventSummary");
            if (summary != null) builder.setEventSummary(summary.toString());
        }
        return builder.build();
    }

    private static String nullToEmpty(String v) { return v != null ? v : ""; }
    private static boolean isBlank(String s) { return s == null || s.isBlank(); }
}
