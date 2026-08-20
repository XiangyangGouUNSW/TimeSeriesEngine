package com.sfkg.timeseries.dataingest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sfkg.timeseries.common.BusinessException;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Component;

/** Converts the normalized DataIngest records back into service entities. */
@Component
public class DataIngestRecordLoader {

    private final DataIngestClient dataIngestClient;
    private final ObjectMapper objectMapper;

    public DataIngestRecordLoader(DataIngestClient dataIngestClient, ObjectMapper objectMapper) {
        this.dataIngestClient = dataIngestClient;
        this.objectMapper = objectMapper;
    }

    public <T> List<T> load(String projectId, String databaseName, String tableName, Class<T> entityType) {
        DataIngestRecordsResponse response = dataIngestClient.queryRecords(databaseName, tableName);
        List<T> entities = new ArrayList<>();
        for (DataIngestRecord record : response.getRecords()) {
            if (record.getRecord() == null || record.getRecord().isNull()) {
                continue;
            }
            try {
                ObjectNode entity = record.getRecord().deepCopy();
                entity.put("projectId", projectId);
                entities.add(objectMapper.treeToValue(entity, entityType));
            } catch (JsonProcessingException exception) {
                throw new BusinessException("parse gStore record failed: table=" + tableName
                        + ", key=" + record.getBusinessKey());
            }
        }
        return entities;
    }
}
