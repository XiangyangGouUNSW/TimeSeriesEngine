package com.sfkg.timeseries.dataingest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DataIngestInsertResponse {

    private Boolean success;

    @JsonProperty("db_name")
    private String dbName;

    private Integer entities;
    private Integer relations;
    private Integer triples;
    private String message;
    private String error;

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public Integer getEntities() {
        return entities;
    }

    public void setEntities(Integer entities) {
        this.entities = entities;
    }

    public Integer getRelations() {
        return relations;
    }

    public void setRelations(Integer relations) {
        this.relations = relations;
    }

    public Integer getTriples() {
        return triples;
    }

    public void setTriples(Integer triples) {
        this.triples = triples;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public boolean isSuccess() {
        return Boolean.TRUE.equals(success);
    }

    public String summary() {
        if (message != null && !message.isBlank()) {
            return message;
        }
        if (error != null && !error.isBlank()) {
            return error;
        }
        return "success=" + success + ", triples=" + triples;
    }
}
