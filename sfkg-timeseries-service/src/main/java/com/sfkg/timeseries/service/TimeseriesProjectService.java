package com.sfkg.timeseries.service;

import com.sfkg.timeseries.dto.ProjectSaveRequest;
import com.sfkg.timeseries.entity.TimeseriesProject;
import java.util.List;

public interface TimeseriesProjectService {

    TimeseriesProject createProject(ProjectSaveRequest request);

    List<TimeseriesProject> listProjects();
}
