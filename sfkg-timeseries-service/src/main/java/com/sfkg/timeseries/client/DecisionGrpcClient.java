package com.sfkg.timeseries.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sfkg.timeseries.config.GrpcClientProperties;
import com.sfkg.timeseries.dto.DecisionContext;
import com.sfkg.timeseries.grpc.DecisionContextRequest;
import com.sfkg.timeseries.grpc.DecisionSuggestionResponse;
import com.sfkg.timeseries.grpc.DiagnosisResultResponse;
import com.sfkg.timeseries.grpc.TimeseriesDecisionServiceGrpc;
import com.sfkg.timeseries.vo.DecisionSuggestionVO;
import com.sfkg.timeseries.vo.DiagnosisResultVO;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DecisionGrpcClient {

    private static final Logger LOG = LoggerFactory.getLogger(DecisionGrpcClient.class);
    private static final String SERVICE_NAME = "timeseries-decision";

    private final GrpcClientProperties grpcClientProperties;
    private final ObjectMapper objectMapper;

    public DecisionGrpcClient(GrpcClientProperties grpcClientProperties, ObjectMapper objectMapper) {
        this.grpcClientProperties = grpcClientProperties;
        this.objectMapper = objectMapper;
    }

    public DiagnosisResultVO generateDiagnosisResult(DecisionContext context) {
        String address = grpcClientProperties.getDecisionAddress();
        if (isBlank(address)) {
            LOG.info("[{}] generateDiagnosisResult skipped: address not configured", SERVICE_NAME);
            return new DiagnosisResultVO();
        }
        if (context == null) {
            return new DiagnosisResultVO();
        }
        DecisionContextRequest req = buildRequest(context);
        LOG.info("[{}] -> generateDiagnosisResult eventId={} at {}", SERVICE_NAME, context.getEventId(), address);

        ManagedChannel channel = newChannel(address);
        try {
            DiagnosisResultResponse resp = TimeseriesDecisionServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .generateDiagnosisResult(req);
            LOG.info("[{}] <- generateDiagnosisResult eventId={} result={} chars", SERVICE_NAME,
                    context.getEventId(), resp.getDiagnosisResult() != null ? resp.getDiagnosisResult().length() : 0);
            DiagnosisResultVO vo = new DiagnosisResultVO();
            vo.setEventId(context.getEventId());
            vo.setDiagnosisResult(resp.getDiagnosisResult());
            vo.setDiagnosisBasis(resp.getDiagnosisBasis());
            return vo;
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] generateDiagnosisResult FAILED: {}", SERVICE_NAME, e.getStatus().getDescription());
            return new DiagnosisResultVO();
        } finally {
            channel.shutdown();
        }
    }

    public DecisionSuggestionVO generateDecisionSuggestion(DecisionContext context) {
        String address = grpcClientProperties.getDecisionAddress();
        if (isBlank(address)) {
            LOG.info("[{}] generateDecisionSuggestion skipped: address not configured", SERVICE_NAME);
            return new DecisionSuggestionVO();
        }
        if (context == null) {
            return new DecisionSuggestionVO();
        }
        DecisionContextRequest req = buildRequest(context);
        LOG.info("[{}] -> generateDecisionSuggestion eventId={} at {}", SERVICE_NAME, context.getEventId(), address);

        ManagedChannel channel = newChannel(address);
        try {
            DecisionSuggestionResponse resp = TimeseriesDecisionServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .generateDecisionSuggestion(req);
            LOG.info("[{}] <- generateDecisionSuggestion eventId={} suggestion={} chars", SERVICE_NAME,
                    context.getEventId(), resp.getSuggestion() != null ? resp.getSuggestion().length() : 0);
            DecisionSuggestionVO vo = new DecisionSuggestionVO();
            vo.setEventId(context.getEventId());
            vo.setSuggestion(resp.getSuggestion());
            return vo;
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] generateDecisionSuggestion FAILED: {}", SERVICE_NAME, e.getStatus().getDescription());
            return new DecisionSuggestionVO();
        } finally {
            channel.shutdown();
        }
    }

    // ── helpers ────────────────────────────────────────────────────────

    private DecisionContextRequest buildRequest(DecisionContext context) {
        String contextJson = "";
        try {
            contextJson = objectMapper.writeValueAsString(context);
        } catch (JsonProcessingException e) {
            LOG.warn("[{}] failed to serialize decision context", SERVICE_NAME, e);
        }
        return DecisionContextRequest.newBuilder()
                .setEventId(nullToEmpty(context.getEventId()))
                .setContextJson(contextJson)
                .build();
    }

    private ManagedChannel newChannel(String address) {
        return ManagedChannelBuilder.forTarget(address).usePlaintext().build();
    }

    private static String nullToEmpty(String v) { return v != null ? v : ""; }
    private static boolean isBlank(String s) { return s == null || s.isBlank(); }
}
