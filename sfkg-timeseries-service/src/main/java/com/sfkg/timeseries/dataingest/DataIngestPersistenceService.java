package com.sfkg.timeseries.dataingest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sfkg.timeseries.config.DataIngestProperties;
import com.sfkg.timeseries.entity.TimeseriesProject;
import com.sfkg.timeseries.mapper.TimeseriesProjectMapper;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DataIngestPersistenceService {

    private static final Logger LOG = LoggerFactory.getLogger(DataIngestPersistenceService.class);
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final DataIngestClient dataIngestClient;
    private final DataIngestProperties properties;
    private final ObjectMapper objectMapper;
    private final TimeseriesProjectMapper projectMapper;

    public DataIngestPersistenceService(
            DataIngestClient dataIngestClient,
            DataIngestProperties properties,
            ObjectMapper objectMapper,
            TimeseriesProjectMapper projectMapper) {
        this.dataIngestClient = dataIngestClient;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.projectMapper = projectMapper;
    }

    public void submitRecord(String tableName, String businessKey, Object entity) {
        if (!dataIngestClient.isEnabled() || entity == null || businessKey == null || businessKey.isBlank()) {
            return;
        }
        try {
            Map<String, Object> rawFields = toRawFields(entity);
            String projectId = rawFields.get("projectId") == null
                    ? null : String.valueOf(rawFields.get("projectId"));
            DataIngestInsertPayload payload = newInsertPayload();
            payload.setDbName(properties.databaseForProject(projectId));
            payload.getEntities().add(toEntityPayload(tableName, businessKey, rawFields));
            DataIngestInsertResponse response = dataIngestClient.insert(payload);
            LOG.info("DataIngest write success: table={} key={} db={} entities={} relations={} triples={} message={}",
                    tableName,
                    businessKey,
                    response.getDbName(),
                    response.getEntities(),
                    response.getRelations(),
                    response.getTriples(),
                    response.getMessage());
            registerProject(projectId);
        } catch (RuntimeException exception) {
            LOG.warn("DataIngest dual-write failed: table={} key={} reason={}",
                    tableName, businessKey, exception.getMessage());
        }
    }

    private void registerProject(String projectId) {
        if (projectId == null || projectId.isBlank()) {
            return;
        }
        TimeseriesProject project = new TimeseriesProject();
        project.setProjectId(projectId);
        project.setDatabaseName(properties.databaseForProject(projectId));
        project.setStatus("ACTIVE");
        project.setUpdatedAt(LocalDateTime.now());
        if (projectMapper.selectByProjectId(projectId) == null) {
            project.setCreatedAt(project.getUpdatedAt());
        }
        projectMapper.upsert(project);
    }

    private DataIngestInsertPayload newInsertPayload() {
        DataIngestInsertPayload payload = new DataIngestInsertPayload();
        payload.setDbName(properties.getDatabase());
        return payload;
    }

    private DataIngestEntityPayload toEntityPayload(String tableName, String businessKey,
            Map<String, Object> rawFields) {
        try {
            rawFields.remove("projectId");
            String json = objectMapper.writeValueAsString(rawFields);
            Map<String, Object> entityProperties = new LinkedHashMap<>();
            entityProperties.put("tableName", tableName);
            entityProperties.put("businessKey", businessKey);
            entityProperties.put("recordJson", json);
            rawFields.forEach((key, value) -> {
                Object normalizedValue = normalizePropertyValue(value);
                if (normalizedValue != null) {
                    entityProperties.put("field_" + key, normalizedValue);
                }
            });
            return new DataIngestEntityPayload(
                    tableName + ":" + businessKey,
                    tableName,
                    "timeseries persistent record",
                    entityProperties);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("serialize DataIngest record failed", exception);
        }
    }

    private Map<String, Object> toRawFields(Object entity) {
        try {
            String json = objectMapper.writeValueAsString(entity);
            return objectMapper.readValue(json, MAP_TYPE);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("serialize DataIngest record failed", exception);
        }
    }

    private Object normalizePropertyValue(Object value) {
        if (value == null || value instanceof String || value instanceof Number) {
            return value;
        }
        if (value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof LocalDateTime) {
            return value.toString();
        }
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            return String.valueOf(value);
        }
    }

}
