package com.sfkg.timeseries.controller;

import static com.sfkg.timeseries.common.JsonSuccessResponse.returnSuccess;

import com.sfkg.timeseries.common.ApiResult;
import com.sfkg.timeseries.dto.AnomalyTaskSaveRequest;
import com.sfkg.timeseries.dto.TaskQueryRequest;
import com.sfkg.timeseries.dto.TaskStatusUpdateRequest;
import com.sfkg.timeseries.service.TimeseriesAnomalyTaskService;
import com.sfkg.timeseries.vo.AnomalyTaskVO;
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
@RequestMapping("/api/timeseries/anomaly-tasks")
public class TimeseriesAnomalyTaskController {

    private final TimeseriesAnomalyTaskService anomalyTaskService;

    public TimeseriesAnomalyTaskController(TimeseriesAnomalyTaskService anomalyTaskService) {
        this.anomalyTaskService = anomalyTaskService;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> createAnomalyTask(@RequestBody AnomalyTaskSaveRequest request) {
        anomalyTaskService.saveAnomalyTask(request);
        return returnSuccess("anomaly task create success");
    }

    @PutMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> updateAnomalyTask(@RequestBody AnomalyTaskSaveRequest request) {
        anomalyTaskService.saveAnomalyTask(request);
        return returnSuccess("anomaly task update success");
    }

    @PatchMapping(value = "/status", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> updateAnomalyTaskStatus(@RequestBody TaskStatusUpdateRequest request) {
        anomalyTaskService.updateAnomalyTaskStatus(request);
        return returnSuccess("anomaly task status update success");
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<List<AnomalyTaskVO>> listAnomalyTasks() {
        List<AnomalyTaskVO> data = anomalyTaskService.listAnomalyTasks();
        return returnSuccess("anomaly task list query success", data);
    }

    @PostMapping(value = "/query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<List<AnomalyTaskVO>> listAnomalyTasksByJson(@RequestBody TaskQueryRequest request) {
        List<AnomalyTaskVO> data = anomalyTaskService.listAnomalyTasks(request);
        return returnSuccess("anomaly task list query success", data);
    }
}
