package com.sfkg.timeseries.controller;

import static com.sfkg.timeseries.common.JsonSuccessResponse.returnSuccess;

import com.sfkg.timeseries.common.ApiResult;
import com.sfkg.timeseries.dto.DecisionQueryRequest;
import com.sfkg.timeseries.dto.DisposalFeedbackRequest;
import com.sfkg.timeseries.service.TimeseriesDecisionService;
import com.sfkg.timeseries.vo.DecisionSuggestionVO;
import com.sfkg.timeseries.vo.DiagnosisResultVO;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/timeseries/decision")
public class TimeseriesDecisionController {

    private final TimeseriesDecisionService decisionService;

    public TimeseriesDecisionController(TimeseriesDecisionService decisionService) {
        this.decisionService = decisionService;
    }

    @GetMapping(value = "/diagnosis", produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<DiagnosisResultVO> getDiagnosisResult(@RequestParam String eventId) {
        DiagnosisResultVO data = decisionService.getDiagnosisResult(eventId);
        return returnSuccess("diagnosis query success", data);
    }

    @PostMapping(value = "/diagnosis", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<DiagnosisResultVO> getDiagnosisResultByJson(@RequestBody DecisionQueryRequest request) {
        DiagnosisResultVO data = decisionService.getDiagnosisResult(request.getProjectId(), request.getEventId());
        return returnSuccess("diagnosis query success", data);
    }

    @GetMapping(value = "/suggestion", produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<DecisionSuggestionVO> getDecisionSuggestion(@RequestParam String eventId) {
        DecisionSuggestionVO data = decisionService.getDecisionSuggestion(eventId);
        return returnSuccess("decision suggestion query success", data);
    }

    @PostMapping(value = "/suggestion", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<DecisionSuggestionVO> getDecisionSuggestionByJson(@RequestBody DecisionQueryRequest request) {
        DecisionSuggestionVO data = decisionService.getDecisionSuggestion(request.getProjectId(), request.getEventId());
        return returnSuccess("decision suggestion query success", data);
    }

    @PatchMapping(value = "/feedback", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> submitDisposalFeedback(@RequestBody DisposalFeedbackRequest request) {
        decisionService.submitDisposalFeedback(request);
        return returnSuccess("disposal feedback submit success");
    }
}
