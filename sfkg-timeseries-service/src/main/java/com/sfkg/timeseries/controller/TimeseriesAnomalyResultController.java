package com.sfkg.timeseries.controller;

import static com.sfkg.timeseries.common.JsonSuccessResponse.returnSuccess;

import com.sfkg.timeseries.common.ApiResult;
import com.sfkg.timeseries.dto.AnomalyResultQueryRequest;
import com.sfkg.timeseries.service.TimeseriesAnomalyResultService;
import com.sfkg.timeseries.vo.AnomalyResultVO;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/timeseries/anomaly-results")
public class TimeseriesAnomalyResultController {

    private final TimeseriesAnomalyResultService anomalyResultService;

    public TimeseriesAnomalyResultController(TimeseriesAnomalyResultService anomalyResultService) {
        this.anomalyResultService = anomalyResultService;
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<AnomalyResultVO> queryAnomalyResults(
            @RequestParam(required = false) String projectId) {
        AnomalyResultQueryRequest request = projectId == null || projectId.isBlank()
                ? null
                : new AnomalyResultQueryRequest();
        if (request != null) request.setProjectId(projectId);
        AnomalyResultVO data = anomalyResultService.queryAnomalyResults(request);
        return returnSuccess("anomaly result query success", data);
    }

    @PostMapping(value = "/query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<AnomalyResultVO> queryAnomalyResultsByJson(@RequestBody AnomalyResultQueryRequest request) {
        AnomalyResultVO data = anomalyResultService.queryAnomalyResults(request);
        return returnSuccess("anomaly result query success", data);
    }
}
