package com.sfkg.timeseries.dataingest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DataIngestRecordsResponse {

    private Boolean success;

    @JsonProperty("db_name")
    private String dbName;

    @JsonProperty("table_name")
    private String tableName;

    private List<DataIngestRecord> records = new ArrayList<>();
    private String error;

    public boolean isSuccess() {
        return Boolean.TRUE.equals(success);
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<DataIngestRecord> getRecords() {
        return records == null ? List.of() : records;
    }

    public void setRecords(List<DataIngestRecord> records) {
        this.records = records == null ? new ArrayList<>() : records;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}
