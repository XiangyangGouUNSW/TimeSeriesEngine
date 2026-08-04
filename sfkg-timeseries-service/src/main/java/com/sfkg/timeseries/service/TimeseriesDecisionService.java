package com.sfkg.timeseries.service;

import com.sfkg.timeseries.dto.DecisionContext;
import com.sfkg.timeseries.dto.DisposalFeedbackRequest;
import com.sfkg.timeseries.vo.DecisionSuggestionVO;
import com.sfkg.timeseries.vo.DiagnosisResultVO;

public interface TimeseriesDecisionService {

    DiagnosisResultVO getDiagnosisResult(String eventId);

    DecisionSuggestionVO getDecisionSuggestion(String eventId);

    void submitDisposalFeedback(DisposalFeedbackRequest request);

    DecisionContext buildDecisionContext(String eventId);

    void syncFeedbackToGraph(String eventId);
}
