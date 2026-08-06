package com.sfkg.timeseries.controller;

import static com.sfkg.timeseries.common.JsonSuccessResponse.returnSuccess;

import com.sfkg.timeseries.common.ApiResult;
import com.sfkg.timeseries.dto.ForecastTaskSaveRequest;
import com.sfkg.timeseries.dto.TaskQueryRequest;
import com.sfkg.timeseries.dto.TaskStatusUpdateRequest;
import com.sfkg.timeseries.service.TimeseriesForecastTaskService;
import com.sfkg.timeseries.vo.ForecastTaskVO;
import java.util.List;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/timeseries/forecast-tasks")
public class TimeseriesForecastTaskController {

    private final TimeseriesForecastTaskService forecastTaskService;

    public TimeseriesForecastTaskController(TimeseriesForecastTaskService forecastTaskService) {
        this.forecastTaskService = forecastTaskService;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> createForecastTask(@RequestBody ForecastTaskSaveRequest request) {
        forecastTaskService.saveForecastTask(request);
        return returnSuccess("forecast task create success");
    }

    @PutMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> updateForecastTask(@RequestBody ForecastTaskSaveRequest request) {
        forecastTaskService.saveForecastTask(request);
        return returnSuccess("forecast task update success");
    }

    @PatchMapping(value = "/status", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> updateForecastTaskStatus(@RequestBody TaskStatusUpdateRequest request) {
        forecastTaskService.updateForecastTaskStatus(request);
        return returnSuccess("forecast task status update success");
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<List<ForecastTaskVO>> listForecastTasks() {
        List<ForecastTaskVO> data = forecastTaskService.listForecastTasks();
        return returnSuccess("forecast task list query success", data);
    }

    @PostMapping(value = "/query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<List<ForecastTaskVO>> listForecastTasksByJson(@RequestBody TaskQueryRequest request) {
        List<ForecastTaskVO> data = forecastTaskService.listForecastTasks(request);
        return returnSuccess("forecast task list query success", data);
    }
}
