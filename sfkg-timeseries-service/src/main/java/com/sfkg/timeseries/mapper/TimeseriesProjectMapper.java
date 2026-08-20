package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesProject;
import java.util.List;

public interface TimeseriesProjectMapper {

    List<TimeseriesProject> selectAll();

    List<TimeseriesProject> selectActiveProjects();

    TimeseriesProject selectByProjectId(String projectId);

    void upsert(TimeseriesProject project);

    void updateStatus(String projectId, String status);
}
