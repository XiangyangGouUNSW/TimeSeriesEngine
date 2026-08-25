package com.sfkg.timeseries.service.impl;

import com.sfkg.timeseries.cache.TimeseriesProjectRegistry;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.common.ProjectIdValidator;
import com.sfkg.timeseries.config.DataIngestProperties;
import com.sfkg.timeseries.dto.ProjectSaveRequest;
import com.sfkg.timeseries.entity.TimeseriesProject;
import com.sfkg.timeseries.mapper.TimeseriesProjectMapper;
import com.sfkg.timeseries.service.TimeseriesProjectService;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.stereotype.Service;

@Service
public class TimeseriesProjectServiceImpl implements TimeseriesProjectService {

    private final TimeseriesProjectMapper projectMapper;
    private final DataIngestProperties dataIngestProperties;
    private final TimeseriesProjectRegistry projectRegistry;

    public TimeseriesProjectServiceImpl(
            TimeseriesProjectMapper projectMapper,
            DataIngestProperties dataIngestProperties,
            TimeseriesProjectRegistry projectRegistry) {
        this.projectMapper = projectMapper;
        this.dataIngestProperties = dataIngestProperties;
        this.projectRegistry = projectRegistry;
    }

    @Override
    public TimeseriesProject createProject(ProjectSaveRequest request) {
        if (request == null) {
            throw new BusinessException("project request must not be null");
        }
        String projectId = ProjectIdValidator.require(request.getProjectId());
        if (projectMapper.selectByProjectId(projectId) != null) {
            throw new BusinessException("project already exists: " + projectId);
        }

        LocalDateTime now = LocalDateTime.now();
        TimeseriesProject project = new TimeseriesProject();
        project.setProjectId(projectId);
        project.setDatabaseName(dataIngestProperties.databaseForProject(projectId));
        project.setStatus("ACTIVE");
        project.setCreatedAt(now);
        project.setUpdatedAt(now);
        projectMapper.upsert(project);
        projectRegistry.register(projectId);
        return project;
    }

    @Override
    public List<TimeseriesProject> listProjects() {
        return projectMapper.selectAll();
    }
}
