package com.sfkg.timeseries.controller;

import static com.sfkg.timeseries.common.JsonSuccessResponse.returnSuccess;

import com.sfkg.timeseries.common.ApiResult;
import com.sfkg.timeseries.dto.DecisionQueryRequest;
import com.sfkg.timeseries.dto.DisposalFeedbackRequest;
import com.sfkg.timeseries.service.TimeseriesDecisionService;
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
    public ApiResult<Void> getDiagnosisResult(@RequestParam String eventId) {
        decisionService.getDiagnosisResult(eventId);
        return returnSuccess("diagnosis query success");
    }

    @PostMapping(value = "/diagnosis", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> getDiagnosisResultByJson(@RequestBody DecisionQueryRequest request) {
        decisionService.getDiagnosisResult(request.getEventId());
        return returnSuccess("diagnosis query success");
    }

    @GetMapping(value = "/suggestion", produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> getDecisionSuggestion(@RequestParam String eventId) {
        decisionService.getDecisionSuggestion(eventId);
        return returnSuccess("decision suggestion query success");
    }

    @PostMapping(value = "/suggestion", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> getDecisionSuggestionByJson(@RequestBody DecisionQueryRequest request) {
        decisionService.getDecisionSuggestion(request.getEventId());
        return returnSuccess("decision suggestion query success");
    }

    @PatchMapping(value = "/feedback", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> submitDisposalFeedback(@RequestBody DisposalFeedbackRequest request) {
        decisionService.submitDisposalFeedback(request);
        return returnSuccess("disposal feedback submit success");
    }
}
