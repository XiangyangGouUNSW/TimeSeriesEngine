package com.sfkg.timeseries.controller;

import static com.sfkg.timeseries.common.JsonSuccessResponse.returnSuccess;

import com.sfkg.timeseries.common.ApiResult;
import com.sfkg.timeseries.dto.InstanceConfigQueryRequest;
import com.sfkg.timeseries.dto.InstanceConfigSaveRequest;
import com.sfkg.timeseries.service.TimeseriesInstanceService;
import com.sfkg.timeseries.vo.InstanceConfigVO;
import java.util.List;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/timeseries/instances")
public class TimeseriesInstanceController {

    private final TimeseriesInstanceService instanceService;

    public TimeseriesInstanceController(TimeseriesInstanceService instanceService) {
        this.instanceService = instanceService;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> createInstanceConfig(@RequestBody InstanceConfigSaveRequest request) {
        instanceService.createInstanceConfig(request);
        return returnSuccess("timeseries instance create success");
    }

    @PutMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> updateInstanceConfig(@RequestBody InstanceConfigSaveRequest request) {
        instanceService.saveInstanceConfig(request);
        return returnSuccess("timeseries instance update success");
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<List<InstanceConfigVO>> queryInstanceConfigs() {
        List<InstanceConfigVO> data = instanceService.queryInstanceConfigs(null);
        return returnSuccess("timeseries instance query success", data);
    }

    @PostMapping(value = "/query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<List<InstanceConfigVO>> queryInstanceConfigsByJson(@RequestBody InstanceConfigQueryRequest request) {
        List<InstanceConfigVO> data = instanceService.queryInstanceConfigs(request);
        return returnSuccess("timeseries instance query success", data);
    }
}
