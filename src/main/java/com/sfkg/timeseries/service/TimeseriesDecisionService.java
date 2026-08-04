package com.sfkg.timeseries.service;

import com.sfkg.timeseries.dto.DecisionContext;
import com.sfkg.timeseries.dto.DisposalFeedbackRequest;
import com.sfkg.timeseries.vo.DecisionSuggestionVO;
import com.sfkg.timeseries.vo.DiagnosisResultVO;

public interface TimeseriesDecisionService {

    DiagnosisResultVO getDiagnosisResult(Integer eventId);

    DecisionSuggestionVO getDecisionSuggestion(Integer eventId);

    void submitDisposalFeedback(DisposalFeedbackRequest request);

    DecisionContext buildDecisionContext(Integer eventId);

    void syncFeedbackToGraph(Integer eventId);
}
