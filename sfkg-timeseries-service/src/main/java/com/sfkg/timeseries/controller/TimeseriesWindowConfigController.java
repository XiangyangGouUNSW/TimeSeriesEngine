package com.sfkg.timeseries.controller;

import static com.sfkg.timeseries.common.JsonSuccessResponse.returnSuccess;

import com.sfkg.timeseries.client.TimeseriesCoreGrpcClient;
import com.sfkg.timeseries.common.ApiResult;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.common.DownstreamSyncValidator;
import com.sfkg.timeseries.common.ProjectIdValidator;
import com.sfkg.timeseries.common.WindowConfigValidator;
import com.sfkg.timeseries.dto.WindowConfigSaveRequest;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/timeseries/window-config")
public class TimeseriesWindowConfigController {

    private final TimeseriesCoreGrpcClient coreGrpcClient;

    public TimeseriesWindowConfigController(TimeseriesCoreGrpcClient coreGrpcClient) {
        this.coreGrpcClient = coreGrpcClient;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> syncWindowConfig(@RequestBody WindowConfigSaveRequest request) {
        WindowConfigValidator.validate(request);
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        DownstreamSyncValidator.requireSuccess("Core",
                coreGrpcClient.syncWindowConfig(request.getProjectId(), request.getWindowSizeMs()));
        return returnSuccess("window config sync success");
    }
}
