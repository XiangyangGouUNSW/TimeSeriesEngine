package com.sfkg.timeseries.service;

import com.sfkg.timeseries.dto.DecisionContext;
import com.sfkg.timeseries.dto.DisposalFeedbackRequest;
import com.sfkg.timeseries.vo.DecisionSuggestionVO;
import com.sfkg.timeseries.vo.DiagnosisResultVO;

public interface TimeseriesDecisionService {

    DiagnosisResultVO getDiagnosisResult(String eventId);

    DiagnosisResultVO getDiagnosisResult(String projectId, String eventId);

    DecisionSuggestionVO getDecisionSuggestion(String eventId);

    DecisionSuggestionVO getDecisionSuggestion(String projectId, String eventId);

    void submitDisposalFeedback(DisposalFeedbackRequest request);

    DecisionContext buildDecisionContext(String eventId);

    DecisionContext buildDecisionContext(String projectId, String eventId);

    void syncFeedbackToGraph(String eventId);
}
