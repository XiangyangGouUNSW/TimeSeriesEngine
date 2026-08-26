package com.sfkg.timeseries.dataingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sfkg.timeseries.config.DataIngestProperties;
import com.sfkg.timeseries.entity.TimeseriesCategory;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.entity.TimeseriesProject;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.mapper.TimeseriesProjectFileMapper;
import com.sfkg.timeseries.dataingest.DataIngestPersistenceService;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DataIngestRecordLoaderTests {

    @Test
    void projectCatalogIsStoredInLocalJson(@TempDir Path tempDir) {
        TimeseriesProjectFileMapper mapper = new TimeseriesProjectFileMapper(
                tempDir.resolve("timeseries-projects.json").toString());
        TimeseriesProject project = new TimeseriesProject();
        project.setProjectId("project-a");
        project.setDatabaseName("ett_system_project-a");
        project.setStatus("ACTIVE");

        mapper.upsert(project);

        assertEquals("ett_system_project-a", mapper.selectByProjectId("project-a").getDatabaseName());
        assertEquals(1, mapper.selectActiveProjects().size());
    }

    @Test
    void recordsAreReadAndProjectIdIsRestored() throws Exception {
        String response = """
                {"success":true,"db_name":"ett_system_project-a","table_name":"timeseries_category",
                 "records":[{"business_key":"cat001","record":{"categoryId":"cat001","categoryName":"Temperature"}}]}
                """;
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/records", exchange -> {
            byte[] body = response.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) {
                output.write(body);
            }
        });
        server.start();
        try {
            DataIngestProperties properties = new DataIngestProperties();
            properties.setEndpoint("http://127.0.0.1:" + server.getAddress().getPort());
            ObjectMapper objectMapper = objectMapper();
            DataIngestClient client = new DataIngestClient(properties, objectMapper);
            DataIngestRecordLoader loader = new DataIngestRecordLoader(client, objectMapper);

            List<TimeseriesCategory> categories = loader.load(
                    "project-a",
                    "ett_system_project-a",
                    "timeseries_category",
                    TimeseriesCategory.class);

            assertEquals(1, categories.size());
            assertEquals("project-a", categories.get(0).getProjectId());
            assertEquals("cat001", categories.get(0).getCategoryId());
            assertTrue(categories.get(0).getCategoryName().contains("Temperature"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void successfulWriteRegistersProjectDatabaseMapping() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/update", exchange -> {
            byte[] body = """
                    {"success":true,"db_name":"ett_system_project-a","entities":1,
                     "relations":0,"triples":4,"message":"ok"}
                    """.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) {
                output.write(body);
            }
        });
        server.start();
        try {
            DataIngestProperties properties = new DataIngestProperties();
            properties.setEnabled(true);
            properties.setEndpoint("http://127.0.0.1:" + server.getAddress().getPort());
            properties.setDatabase("ett_system");
            ObjectMapper objectMapper = objectMapper();
            DataIngestClient client = new DataIngestClient(properties, objectMapper);
            TimeseriesProjectFileMapper projectMapper = new TimeseriesProjectFileMapper(
                    java.nio.file.Files.createTempFile("timeseries-projects", ".json").toString());
            DataIngestPersistenceService persistenceService = new DataIngestPersistenceService(
                    client, properties, objectMapper, projectMapper);

            TimeseriesCategory category = new TimeseriesCategory();
            category.setProjectId("project-a");
            category.setCategoryId("cat001");
            category.setCategoryName("Temperature");

            persistenceService.submitRecord("timeseries_category", "cat001", category);

            TimeseriesProject project = projectMapper.selectByProjectId("project-a");
            assertEquals("ett_system_project-a", project.getDatabaseName());
            assertEquals("ACTIVE", project.getStatus());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void semanticRelationsAreIncludedInPersistenceRequest() throws Exception {
        AtomicReference<String> requestBody = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/update", exchange -> {
            requestBody.set(new String(exchange.getRequestBody().readAllBytes(),
                    java.nio.charset.StandardCharsets.UTF_8));
            byte[] body = "{\"success\":true,\"db_name\":\"ett_system_project-a\",\"entities\":1,\"relations\":1,\"triples\":5,\"message\":\"ok\"}"
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) {
                output.write(body);
            }
        });
        server.start();
        try {
            DataIngestProperties properties = new DataIngestProperties();
            properties.setEnabled(true);
            properties.setEndpoint("http://127.0.0.1:" + server.getAddress().getPort());
            properties.setDatabase("ett_system");
            ObjectMapper objectMapper = objectMapper();
            DataIngestClient client = new DataIngestClient(properties, objectMapper);
            TimeseriesProjectFileMapper projectMapper = new TimeseriesProjectFileMapper(
                    java.nio.file.Files.createTempFile("timeseries-projects", ".json").toString());
            TimeseriesMemoryCache cache = new TimeseriesMemoryCache();
            TimeseriesCategory category = new TimeseriesCategory();
            category.setProjectId("project-a");
            category.setCategoryId("cat001");
            cache.putCategory(category);

            DataIngestPersistenceService persistenceService = new DataIngestPersistenceService(
                    client, properties, objectMapper, projectMapper, cache);
            TimeseriesInstanceConfig instance = new TimeseriesInstanceConfig();
            instance.setProjectId("project-a");
            instance.setSequenceId("1001");
            instance.setCategoryId("cat001");

            persistenceService.submitRecord("timeseries_instance_config", "1001", instance);

            var payload = objectMapper.readTree(requestBody.get());
            assertTrue(payload.path("preserve_relations").asBoolean());
            assertEquals(1, payload.path("relations").size());
            assertEquals("timeseries_instance_config:1001",
                    payload.path("relations").get(0).path("source").asText());
            assertEquals("belongs_to_category",
                    payload.path("relations").get(0).path("type").asText());
            assertEquals("timeseries_category:cat001",
                    payload.path("relations").get(0).path("target").asText());
        } finally {
            server.stop(0);
        }
    }

    private ObjectMapper objectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }
}
