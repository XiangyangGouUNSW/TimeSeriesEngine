package com.sfkg.timeseries.dataingest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sfkg.timeseries.config.DataIngestProperties;
import com.sfkg.timeseries.common.ProjectIdValidator;
import com.sfkg.timeseries.entity.TimeseriesProject;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
import com.sfkg.timeseries.entity.TimeseriesEvent;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.entity.TimeseriesRelation;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.mapper.TimeseriesProjectMapper;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
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
    private final TimeseriesMemoryCache memoryCache;

    @Autowired
    public DataIngestPersistenceService(
            DataIngestClient dataIngestClient,
            DataIngestProperties properties,
            ObjectMapper objectMapper,
            TimeseriesProjectMapper projectMapper,
            TimeseriesMemoryCache memoryCache) {
        this.dataIngestClient = dataIngestClient;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.projectMapper = projectMapper;
        this.memoryCache = memoryCache;
    }

    /** Backward-compatible constructor for mapper-focused tests and callers. */
    public DataIngestPersistenceService(
            DataIngestClient dataIngestClient,
            DataIngestProperties properties,
            ObjectMapper objectMapper,
            TimeseriesProjectMapper projectMapper) {
        this(dataIngestClient, properties, objectMapper, projectMapper, null);
    }

    public void submitRecord(String tableName, String businessKey, Object entity) {
        if (entity == null) {
            return;
        }
        try {
            Map<String, Object> rawFields = toRawFields(entity);
            String projectId = ProjectIdValidator.require(
                    rawFields.get("projectId") == null ? null : String.valueOf(rawFields.get("projectId")));
            if (!dataIngestClient.isEnabled() || businessKey == null || businessKey.isBlank()) {
                return;
            }
            DataIngestInsertPayload payload = newInsertPayload();
            payload.setDbName(properties.databaseForProject(projectId));
            payload.getEntities().add(toEntityPayload(tableName, businessKey, rawFields));
            payload.getRelations().addAll(buildRelations(tableName, businessKey, entity, projectId));
            DataIngestInsertResponse response = dataIngestClient.update(payload);
            LOG.info("DataIngest upsert success: table={} key={} db={} entities={} relations={} triples={} message={}",
                    tableName,
                    businessKey,
                    response.getDbName(),
                    response.getEntities(),
                    response.getRelations(),
                    response.getTriples(),
                    response.getMessage());
            registerProject(projectId);
        } catch (RuntimeException exception) {
            LOG.warn("DataIngest upsert failed: table={} key={} reason={}",
                    tableName, businessKey, exception.getMessage());
            throw exception;
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
        payload.setPreserveRelations(true);
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

    /**
     * Build the semantic graph edges owned by the entity being persisted.
     * The record JSON remains the source used by /records; these relations are
     * additional RDF triples for graph traversal and reasoning.
     */
    private List<DataIngestRelationPayload> buildRelations(
            String tableName, String businessKey, Object entity, String projectId) {
        String source = nodeName(tableName, businessKey);
        List<DataIngestRelationPayload> relations = new ArrayList<>();
        Set<String> keys = new HashSet<>();

        if (entity instanceof TimeseriesInstanceConfig instance) {
            addRelation(relations, keys, source, "belongs_to_category",
                    nodeName("timeseries_category", instance.getCategoryId()));
        } else if (entity instanceof TimeseriesConstraint constraint) {
            if (constraint.getVariableMapping() != null) {
                for (String referenceId : constraint.getVariableMapping().values()) {
                    SemanticReference reference = resolveSemanticReference(projectId, referenceId);
                    if (reference != null) {
                        addRelation(relations, keys, source,
                                reference.category() ? "applies_to_category" : "applies_to_instance",
                                reference.nodeName());
                    }
                }
            }
        } else if (entity instanceof TimeseriesRelation relation) {
            if (relation.getSourceSequences() != null) {
                for (String referenceId : relation.getSourceSequences()) {
                    SemanticReference reference = resolveSemanticReference(projectId, referenceId);
                    if (reference != null) {
                        addRelation(relations, keys, source, "has_source", reference.nodeName());
                    }
                }
            }
            SemanticReference target = resolveSemanticReference(projectId, relation.getTargetSequenceId());
            if (target != null) {
                addRelation(relations, keys, source, "has_target", target.nodeName());
            }
        } else if (entity instanceof TimeseriesEvent event) {
            if (event.getRelatedSequences() != null) {
                for (String sequenceId : event.getRelatedSequences()) {
                    addRelation(relations, keys, source,
                            "involves_sequence", nodeName("timeseries_instance_config", sequenceId));
                }
            }
            if (event.getRelatedRules() != null) {
                for (String constraintId : event.getRelatedRules()) {
                    addRelation(relations, keys, source,
                            "violates_constraint", nodeName("timeseries_constraint", constraintId));
                }
            }
        }
        return relations;
    }

    private SemanticReference resolveSemanticReference(String projectId, String referenceId) {
        if (referenceId == null || referenceId.isBlank()) {
            return null;
        }
        if (memoryCache != null
                && memoryCache.getCategory(projectId, referenceId).isPresent()) {
            return new SemanticReference("timeseries_category", referenceId, true);
        }
        return new SemanticReference("timeseries_instance_config", referenceId, false);
    }

    private void addRelation(
            List<DataIngestRelationPayload> relations,
            Set<String> keys,
            String source,
            String type,
            String target) {
        if (source == null || type == null || target == null
                || source.isBlank() || type.isBlank() || target.isBlank()) {
            return;
        }
        String key = source + "\u0000" + type + "\u0000" + target;
        if (!keys.add(key)) {
            return;
        }
        DataIngestRelationPayload relation = new DataIngestRelationPayload();
        relation.setSource(source);
        relation.setType(type);
        relation.setTarget(target);
        relations.add(relation);
    }

    private String nodeName(String tableName, String businessKey) {
        if (tableName == null || tableName.isBlank()
                || businessKey == null || businessKey.isBlank()) {
            return null;
        }
        return tableName + ":" + businessKey;
    }

    private record SemanticReference(String tableName, String id, boolean category) {
        private String nodeName() {
            return tableName + ":" + id;
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
