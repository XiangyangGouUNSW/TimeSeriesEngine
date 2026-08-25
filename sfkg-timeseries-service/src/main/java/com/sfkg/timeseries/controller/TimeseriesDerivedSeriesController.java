package com.sfkg.timeseries.controller;

import static com.sfkg.timeseries.common.JsonSuccessResponse.returnSuccess;

import com.sfkg.timeseries.client.TimeseriesCoreGrpcClient;
import com.sfkg.timeseries.common.ApiResult;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.common.ProjectIdValidator;
import com.sfkg.timeseries.dto.DerivedSeriesConfigSaveRequest;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/timeseries/derived-series")
public class TimeseriesDerivedSeriesController {

    private final TimeseriesCoreGrpcClient coreGrpcClient;

    public TimeseriesDerivedSeriesController(TimeseriesCoreGrpcClient coreGrpcClient) {
        this.coreGrpcClient = coreGrpcClient;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> syncDerivedSeries(@RequestBody DerivedSeriesConfigSaveRequest request) {
        if (request == null || request.getItems() == null || request.getItems().isEmpty()) {
            throw new BusinessException("derived series items must not be empty");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        for (DerivedSeriesConfigSaveRequest.DerivedSeriesConfigItem item : request.getItems()) {
            if (item.getDerivedSequenceId() == null || item.getDerivedSequenceId().isBlank()) {
                throw new BusinessException("derivedSequenceId must not be empty");
            }
            if (item.getLinearCombination() == null && item.getExpression() == null) {
                throw new BusinessException(
                        "derived series must have linearCombination or expression: " + item.getDerivedSequenceId());
            }
        }
        coreGrpcClient.syncDerivedSeriesConfigs(request);
        return returnSuccess("derived series config sync success");
    }
}
