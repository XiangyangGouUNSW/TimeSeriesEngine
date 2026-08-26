package com.sfkg.timeseries.dataingest;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

public class DataIngestInsertPayload {

    @JsonProperty("db_name")
    private String dbName;
    @JsonProperty("preserve_relations")
    private boolean preserveRelations;
    private List<DataIngestEntityPayload> entities = new ArrayList<>();
    private List<DataIngestRelationPayload> relations = new ArrayList<>();

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public boolean isPreserveRelations() {
        return preserveRelations;
    }

    public void setPreserveRelations(boolean preserveRelations) {
        this.preserveRelations = preserveRelations;
    }

    public List<DataIngestEntityPayload> getEntities() {
        return entities;
    }

    public void setEntities(List<DataIngestEntityPayload> entities) {
        this.entities = entities;
    }

    public List<DataIngestRelationPayload> getRelations() {
        return relations;
    }

    public void setRelations(List<DataIngestRelationPayload> relations) {
        this.relations = relations;
    }
}
