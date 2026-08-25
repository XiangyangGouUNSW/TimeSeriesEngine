package com.sfkg.timeseries.controller;

import static com.sfkg.timeseries.common.JsonSuccessResponse.returnSuccess;

import com.sfkg.timeseries.common.ApiResult;
import com.sfkg.timeseries.dto.ProjectSaveRequest;
import com.sfkg.timeseries.entity.TimeseriesProject;
import com.sfkg.timeseries.service.TimeseriesProjectService;
import java.util.List;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/timeseries/projects")
public class TimeseriesProjectController {

    private final TimeseriesProjectService projectService;

    public TimeseriesProjectController(TimeseriesProjectService projectService) {
        this.projectService = projectService;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<TimeseriesProject> createProject(@RequestBody ProjectSaveRequest request) {
        return returnSuccess("timeseries project create success", projectService.createProject(request));
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<List<TimeseriesProject>> listProjects() {
        return returnSuccess("timeseries project query success", projectService.listProjects());
    }
}
