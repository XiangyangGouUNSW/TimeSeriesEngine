package com.sfkg.timeseries;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.sfkg.timeseries.cache.TimeseriesProjectRegistry;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.config.DataIngestProperties;
import com.sfkg.timeseries.dto.ProjectSaveRequest;
import com.sfkg.timeseries.entity.TimeseriesProject;
import com.sfkg.timeseries.mapper.TimeseriesProjectFileMapper;
import com.sfkg.timeseries.service.TimeseriesProjectService;
import com.sfkg.timeseries.service.impl.TimeseriesProjectServiceImpl;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TimeseriesProjectServiceTests {

    @Test
    void createProjectStoresCatalogAndDatabaseMapping(@TempDir Path tempDir) {
        DataIngestProperties properties = new DataIngestProperties();
        properties.setDatabase("ett-system");
        TimeseriesProjectFileMapper mapper = new TimeseriesProjectFileMapper(
                tempDir.resolve("projects.json").toString());
        TimeseriesProjectService service = new TimeseriesProjectServiceImpl(
                mapper, properties, new TimeseriesProjectRegistry());

        ProjectSaveRequest request = new ProjectSaveRequest();
        request.setProjectId(" project-a ");

        TimeseriesProject project = service.createProject(request);

        assertEquals("project-a", project.getProjectId());
        assertEquals("ett-system_project-a", project.getDatabaseName());
        assertEquals("ACTIVE", project.getStatus());
        assertEquals("project-a", mapper.selectByProjectId("project-a").getProjectId());
    }

    @Test
    void projectIdIsRequiredAndDuplicateIsRejected(@TempDir Path tempDir) {
        DataIngestProperties properties = new DataIngestProperties();
        TimeseriesProjectFileMapper mapper = new TimeseriesProjectFileMapper(
                tempDir.resolve("projects.json").toString());
        TimeseriesProjectService service = new TimeseriesProjectServiceImpl(
                mapper, properties, new TimeseriesProjectRegistry());

        ProjectSaveRequest missing = new ProjectSaveRequest();
        assertThrows(BusinessException.class, () -> service.createProject(missing));

        ProjectSaveRequest first = new ProjectSaveRequest();
        first.setProjectId("project-a");
        service.createProject(first);
        assertThrows(BusinessException.class, () -> service.createProject(first));
    }
}
