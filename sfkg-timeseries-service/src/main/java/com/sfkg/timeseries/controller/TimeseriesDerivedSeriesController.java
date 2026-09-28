package com.sfkg.timeseries.controller;

import static com.sfkg.timeseries.common.JsonSuccessResponse.returnSuccess;

import com.sfkg.timeseries.client.TimeseriesCoreGrpcClient;
import com.sfkg.timeseries.common.ApiResult;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.common.DownstreamSyncValidator;
import com.sfkg.timeseries.common.ProjectIdValidator;
import com.sfkg.timeseries.dto.DerivedSeriesConfigSaveRequest;
import com.sfkg.timeseries.service.DerivedSeriesConfigValidator;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/timeseries/derived-series")
public class TimeseriesDerivedSeriesController {

    private final TimeseriesCoreGrpcClient coreGrpcClient;
    private final DerivedSeriesConfigValidator validator;

    public TimeseriesDerivedSeriesController(TimeseriesCoreGrpcClient coreGrpcClient,
            DerivedSeriesConfigValidator validator) {
        this.coreGrpcClient = coreGrpcClient;
        this.validator = validator;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> syncDerivedSeries(@RequestBody DerivedSeriesConfigSaveRequest request) {
        if (request == null) {
            throw new BusinessException("derived series request must not be null");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        validator.validate(request.getProjectId(), request.getItems());
        DownstreamSyncValidator.requireSuccess("Core", coreGrpcClient.syncDerivedSeriesConfigs(request));
        return returnSuccess("derived series config sync success");
    }
}
