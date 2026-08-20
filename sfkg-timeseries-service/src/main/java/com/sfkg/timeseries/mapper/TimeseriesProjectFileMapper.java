package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesProject;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
public class TimeseriesProjectFileMapper implements TimeseriesProjectMapper {

    private final LocalJsonTableStore<TimeseriesProject> store;

    public TimeseriesProjectFileMapper(
            @Value("${timeseries.project-catalog-file:data/timeseries-projects.json}") String catalogFile) {
        this.store = new LocalJsonTableStore<>(Paths.get(catalogFile), TimeseriesProject.class);
    }

    @Override
    public List<TimeseriesProject> selectAll() {
        return store.readAll();
    }

    @Override
    public List<TimeseriesProject> selectActiveProjects() {
        return store.readAll().stream()
                .filter(project -> "ACTIVE".equalsIgnoreCase(project.getStatus()))
                .toList();
    }

    @Override
    public TimeseriesProject selectByProjectId(String projectId) {
        return store.readAll().stream()
                .filter(project -> projectId != null && projectId.equals(project.getProjectId()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public void upsert(TimeseriesProject project) {
        if (project == null || project.getProjectId() == null || project.getProjectId().isBlank()) {
            throw new IllegalArgumentException("projectId must not be blank");
        }
        if (project.getStatus() == null || project.getStatus().isBlank()) {
            project.setStatus("ACTIVE");
        } else {
            project.setStatus(project.getStatus().toUpperCase(Locale.ROOT));
        }
        store.upsert(item -> project.getProjectId().equals(item.getProjectId()), project);
    }

    @Override
    public void updateStatus(String projectId, String status) {
        if (projectId == null || status == null || status.isBlank()) {
            return;
        }
        store.update(
                project -> projectId.equals(project.getProjectId()),
                project -> project.setStatus(status.toUpperCase(Locale.ROOT)));
    }
}
