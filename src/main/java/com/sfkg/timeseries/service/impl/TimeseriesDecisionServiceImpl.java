package com.sfkg.timeseries.service.impl;

import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.client.DecisionGrpcClient;
import com.sfkg.timeseries.client.TimeseriesCoreGrpcClient;
import com.sfkg.timeseries.dto.DecisionContext;
import com.sfkg.timeseries.dto.DisposalFeedbackRequest;
import com.sfkg.timeseries.entity.TimeseriesEvent;
import com.sfkg.timeseries.mapper.TimeseriesEventMapper;
import com.sfkg.timeseries.service.TimeseriesDecisionService;
import com.sfkg.timeseries.vo.DecisionSuggestionVO;
import com.sfkg.timeseries.vo.DiagnosisResultVO;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.stereotype.Service;

@Service
public class TimeseriesDecisionServiceImpl implements TimeseriesDecisionService {

    private final TimeseriesEventMapper eventMapper;
    private final TimeseriesMemoryCache memoryCache;
    private final TimeseriesCacheManager cacheManager;
    private final TimeseriesCoreGrpcClient coreGrpcClient;
    private final DecisionGrpcClient decisionGrpcClient;

    public TimeseriesDecisionServiceImpl(
            TimeseriesEventMapper eventMapper,
            TimeseriesMemoryCache memoryCache,
            TimeseriesCacheManager cacheManager,
            TimeseriesCoreGrpcClient coreGrpcClient,
            DecisionGrpcClient decisionGrpcClient) {
        this.eventMapper = eventMapper;
        this.memoryCache = memoryCache;
        this.cacheManager = cacheManager;
        this.coreGrpcClient = coreGrpcClient;
        this.decisionGrpcClient = decisionGrpcClient;
    }

    @Override
    public DiagnosisResultVO getDiagnosisResult(Integer eventId) {
        cacheManager.ensureTableLoaded(CachedTable.EVENT);
        DecisionContext context = buildDecisionContext(eventId);
        DiagnosisResultVO vo = decisionGrpcClient.generateDiagnosisResult(context);
        if (vo.getDiagnosisResult() == null) {
            // fallback to event cache
            memoryCache.getEvent(eventId).ifPresent(event -> {
                vo.setEventId(event.getEventId());
                vo.setDiagnosisResult(event.getDiagnosisResult());
                vo.setDiagnosisBasis(event.getDiagnosisBasis());
            });
        }
        return vo;
    }

    @Override
    public DecisionSuggestionVO getDecisionSuggestion(Integer eventId) {
        cacheManager.ensureTableLoaded(CachedTable.EVENT);
        DecisionContext context = buildDecisionContext(eventId);
        DecisionSuggestionVO vo = decisionGrpcClient.generateDecisionSuggestion(context);
        if (vo.getSuggestion() == null) {
            // fallback to event cache
            vo.setEventId(eventId);
            memoryCache.getEvent(eventId).ifPresent(event -> {
                if (event.getDisposalResult() != null) {
                    vo.setSuggestion(event.getDisposalResult());
                } else {
                    vo.setSuggestion("please review event " + event.getEventId());
                }
            });
        }
        return vo;
    }

    @Override
    public void submitDisposalFeedback(DisposalFeedbackRequest request) {
        if (request == null || request.getEventId() == null) {
            return;
        }
        cacheManager.ensureTableLoaded(CachedTable.EVENT);
        TimeseriesEvent entity = memoryCache.getEvent(request.getEventId())
                .orElseGet(TimeseriesEvent::new);
        entity.setEventId(request.getEventId());
        if (request.getDisposalResult() != null) {
            entity.setDisposalResult(request.getDisposalResult());
        }
        if (request.getHandleStatus() != null) {
            entity.setHandleStatus(request.getHandleStatus());
        }

        eventMapper.updateById(entity);
        memoryCache.putEvent(entity);
    }

    @Override
    public DecisionContext buildDecisionContext(Integer eventId) {
        cacheManager.ensureTableLoaded(CachedTable.EVENT);
        DecisionContext context = new DecisionContext();
        context.setEventId(eventId);
        memoryCache.getEvent(eventId).ifPresent(event -> {
            context.setEventInfo(toEventInfo(event));
            context.setSemanticContext(toSemanticContext(event));
            context.setStatisticsContext(Map.of());
        });
        return context;
    }

    @Override
    public void syncFeedbackToGraph(Integer eventId) {
        // TODO: Restore graph synchronization here.
    }

    private Map<String, Object> toEventInfo(TimeseriesEvent event) {
        Map<String, Object> eventInfo = new LinkedHashMap<>();
        eventInfo.put("eventId", event.getEventId());
        eventInfo.put("eventName", event.getEventName());
        eventInfo.put("eventType", event.getEventType());
        eventInfo.put("eventLevel", event.getEventLevel());
        eventInfo.put("eventTime", event.getEventTime());
        eventInfo.put("handleStatus", event.getHandleStatus());
        return eventInfo;
    }

    private Map<String, Object> toSemanticContext(TimeseriesEvent event) {
        Map<String, Object> semanticContext = new LinkedHashMap<>();
        semanticContext.put("relatedSequences", event.getRelatedSequences());
        semanticContext.put("relatedRules", event.getRelatedRules());
        semanticContext.put("diagnosisResult", event.getDiagnosisResult());
        semanticContext.put("diagnosisBasis", event.getDiagnosisBasis());
        semanticContext.put("disposalResult", event.getDisposalResult());
        return semanticContext;
    }
}
